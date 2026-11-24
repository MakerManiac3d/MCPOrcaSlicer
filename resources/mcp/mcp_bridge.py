# /// script
# requires-python = ">=3.12"
# dependencies = []
#
# [tool.orcaslicer.plugin]
# name = "MCP Bridge"
# description = "Control de OrcaSlicer por agentes de IA (MCP): lee la escena, presets y configuración; edita perfiles de proceso, impresora y filamento en vivo; modifica el modelo y lamina en segundo plano."
# author = "makermaniac3d"
# version = "0.6.0"
# ///
"""
MCP Bridge para OrcaSlicer 2.5+ (plugin Python, sin fork).

Arquitectura
------------
  Agente IA ──MCP stdio──▶ servidor MCP externo ──HTTP 127.0.0.1 + token──▶ este plugin

* El servidor HTTP corre DENTRO del script "MCP Bridge: servidor" (async def execute), es decir,
  en el hilo de trabajo que OrcaSlicer da a cada script. Es el único hilo desde el que la API
  permite editar presets (orca.host.preset.<tipo>.begin_update / set / commit).
* Lo que debe leerse en el hilo de la interfaz (modelo, presets, export_3mf_copy, app_info) se
  encola y lo ejecuta la "bomba": el panel acoplado "MCP Bridge" envía un tick cada ~120 ms y
  su on_message (hilo UI) vacía la cola.
* El modelo no tiene API de escritura. Se modifica así: export_3mf_copy() → edición del 3MF
  (o CLI de OrcaSlicer: organizar, orientar, reparar, cortar…) → reapertura en la ventana ya
  abierta con `orca-slicer --single-instance archivo.3mf`.
* Laminado en segundo plano: la CLI de OrcaSlicer sobre una copia del proyecto.

Ampliar (modular)
-----------------
    @bridge_method("mi_modulo.algo", kind="ui", doc="Qué hace")
    def _algo(params): ...

kind: "ui" (hilo de interfaz, lectura del modelo), "worker" (hilo del script; puede ser
`async def` y usar `await on_ui(fn)`), "any" (sin acceso a orca.host).
"""

from __future__ import annotations

import asyncio
import collections
import copy
import inspect
import json
import math
import os
import queue
import re
import secrets
import shutil
import socket
import subprocess
import sys
import time
import traceback
import xml.etree.ElementTree as ET
import zipfile

import orca

BRIDGE_VERSION = "0.5.2"
BRIDGE_FILE_NAME = "orca_mcp_bridge.json"
LISTEN_ADDR = "127.0.0.1"
DEFAULT_PORT = 47615
UI_TIMEOUT_S = 20.0
PUMP_BUDGET_S = 0.05
MAX_BODY_BYTES = 2_000_000
BUNDLE_ID = "mcp_bridge"
PRESET_DIRS = {"process": "process", "filament": "filament", "printer": "machine"}
COLLECTION_ATTR = {"process": "prints", "filament": "filaments", "printer": "printers"}
SETTINGS_ID_KEY = {"process": "print_settings_id", "filament": "filament_settings_id",
                   "printer": "printer_settings_id"}
META_KEYS = {"inherits", "name", "from", "version", "is_custom_defined", "print_settings_id",
             "filament_settings_id", "printer_settings_id", "setting_id", "instantiation"}
DENIED_NAME_PARTS = {"secret", "secrets", "cert", "certs", "certificate", "certificates", "conf", "config"}


# ─────────────────────────────────────────────────────────────────────────────
# Registro de métodos y errores
# ─────────────────────────────────────────────────────────────────────────────

class BridgeError(Exception):
    """Error con mensaje pensado para el agente."""


_METHODS: dict[str, dict] = {}


def bridge_method(name: str, kind: str = "ui", doc: str = "", write: bool = False):
    if kind not in ("ui", "worker", "any"):
        raise ValueError(kind)

    def deco(fn):
        _METHODS[name] = {"fn": fn, "kind": kind, "write": write,
                          "doc": doc or (fn.__doc__ or "").strip()}
        return fn
    return deco


EXTRA_3MF_OPS: dict = {}


def register_3mf_op(name: str):
    """Registra una operación nueva de edit_model: fn(project: Project3MF, op: dict) -> str."""
    def deco(fn):
        EXTRA_3MF_OPS[name] = fn
        return fn
    return deco


class _BridgeAPI:
    """Lo que ven los módulos recargables como `bridge` (acceso a todo el plugin)."""

    def __getattr__(self, name):
        try:
            return globals()[name]
        except KeyError:
            raise AttributeError(name) from None


_MODULE_OWNED: dict = {}  # módulo -> (métodos, operaciones) que registró


def load_modules() -> dict:
    """Carga (o recarga) los .py de <carpeta del plugin>/modules sin reiniciar OrcaSlicer."""
    import importlib.util
    folder = os.path.join(STATE.storage, "modules")
    os.makedirs(folder, exist_ok=True)
    for _mod, (meths, ops) in _MODULE_OWNED.items():
        for m in meths:
            _METHODS.pop(m, None)
        for o in ops:
            EXTRA_3MF_OPS.pop(o, None)
    _MODULE_OWNED.clear()
    report = {"folder": folder, "loaded": [], "errors": {}}
    for fn in sorted(os.listdir(folder)):
        if not fn.endswith(".py") or fn.startswith("_"):
            continue
        before_m, before_o = set(_METHODS), set(EXTRA_3MF_OPS)
        try:
            spec = importlib.util.spec_from_file_location(f"mcp_bridge_mod_{fn[:-3]}", os.path.join(folder, fn))
            mod = importlib.util.module_from_spec(spec)
            mod.bridge = _BridgeAPI()
            spec.loader.exec_module(mod)
            _MODULE_OWNED[fn] = (set(_METHODS) - before_m, set(EXTRA_3MF_OPS) - before_o)
            report["loaded"].append({"file": fn, "methods": sorted(_MODULE_OWNED[fn][0]),
                                     "operations": sorted(_MODULE_OWNED[fn][1])})
        except Exception as exc:  # noqa: BLE001
            report["errors"][fn] = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()
    return report


# ─────────────────────────────────────────────────────────────────────────────
# Estado compartido
# ─────────────────────────────────────────────────────────────────────────────

class _BridgeState:
    def __init__(self):
        self.ui_jobs: "queue.Queue" = queue.Queue()
        self.events = collections.deque(maxlen=500)
        self.event_seq = 0
        self.last_slice: dict | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.stop_event: asyncio.Event | None = None
        self.write_lock: asyncio.Lock | None = None
        self.token = ""
        self.port = 0
        self.storage = ""
        self.bridge_file = ""
        self.panel = None
        self.last_pump = 0.0
        self.served = 0
        self.last_method = ""
        self.last_error = ""
        self.started_at = 0.0
        self.cfg: dict = {}
        self.exe = ""

    def add_event(self, rec: dict):
        self.event_seq += 1
        rec["seq"] = self.event_seq
        rec["t"] = time.time()
        self.events.append(rec)

    def log(self, text: str):
        self.add_event({"event": "bridge_log", "text": text})
        print("[MCP Bridge]", text)

    def pump(self):
        """Hilo UI: ejecuta trabajos encolados durante como mucho PUMP_BUDGET_S."""
        self.last_pump = time.time()
        deadline = time.monotonic() + PUMP_BUDGET_S
        while time.monotonic() < deadline:
            try:
                job = self.ui_jobs.get_nowait()
            except queue.Empty:
                break
            job()

    def pump_alive(self) -> bool:
        return (time.time() - self.last_pump) < 2.0

    @property
    def running(self) -> bool:
        return self.loop is not None


STATE = _BridgeState()


def _direct_ui_call():
    edit = getattr(getattr(orca, "host", None), "edit", None)
    return getattr(edit, "call_on_ui", None)


async def on_ui(fn, *args, timeout: float | None = None):
    """Desde el hilo del script: ejecuta fn(*args) en el hilo de la interfaz y espera el resultado.
    En MCPOrcaSlicer lo hace el propio OrcaSlicer (orca.host.edit.call_on_ui), sin depender del panel."""
    direct = _direct_ui_call()
    if direct is not None:
        return direct(lambda: fn(*args))
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def deliver(result, error):
        if not fut.done():
            fut.set_exception(error) if error is not None else fut.set_result(result)

    def job():
        try:
            res = fn(*args)
            loop.call_soon_threadsafe(deliver, res, None)
        except BaseException as exc:  # noqa: BLE001
            loop.call_soon_threadsafe(deliver, None, exc)

    STATE.ui_jobs.put(job)
    try:
        return await asyncio.wait_for(fut, timeout or UI_TIMEOUT_S)
    except asyncio.TimeoutError:
        hint = "" if STATE.pump_alive() else (
            " El panel 'MCP Bridge' no está activo: ábrelo en la pestaña Preparar o Previsualizar "
            "(menú Plugins) para que el puente pueda leer el modelo.")
        raise BridgeError("Tiempo de espera agotado en el hilo de la interfaz." + hint) from None


# ─────────────────────────────────────────────────────────────────────────────
# Utilidades
# ─────────────────────────────────────────────────────────────────────────────

def _jsonable(value, depth=0):
    if depth > 8:
        return str(value)
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v, depth + 1) for v in value]
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        try:
            return _jsonable(tolist(), depth + 1)
        except Exception:  # noqa: BLE001
            pass
    name = getattr(value, "name", None)
    if isinstance(name, str) and hasattr(type(value), "__members__"):
        return name
    if all(hasattr(value, a) for a in ("min", "max")):
        return {k: _jsonable(_get(value, k), depth + 1) for k in ("min", "max", "size", "center") if hasattr(value, k)}
    return str(value)


def _get(obj, attr, default=None):
    """Lee un atributo o un accesor sin argumentos."""
    try:
        val = getattr(obj, attr)
    except Exception:  # noqa: BLE001
        return default
    if callable(val):
        try:
            return val()
        except Exception:  # noqa: BLE001
            return default
    return val


def serialize_value(value, current: str | None = None) -> str:
    """Convierte un valor JSON al formato serializado de OrcaSlicer (el que acepta set())."""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        out = "1" if value else "0"
    elif isinstance(value, (int, float)):
        out = ("%.10g" % value) if isinstance(value, float) else str(value)
    elif isinstance(value, (list, tuple)):
        if value and all(isinstance(v, str) for v in value) and (current or "").startswith('"'):
            return ";".join('"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"' for v in value)
        return ",".join(serialize_value(v) for v in value)
    else:
        raise BridgeError(f"Tipo de valor no admitido: {type(value).__name__}")
    # Opción vector (p. ej. multiextrusor "220,220"): replica el escalar en cada elemento.
    if current and "," in current and not current.startswith('"'):
        parts = current.split(",")
        if all(re.fullmatch(r"-?\d+(\.\d+)?%?", p.strip()) for p in parts):
            return ",".join([out] * len(parts))
    return out


def _app_version_semver(raw: str) -> str:
    m = re.search(r"(\d+)\.(\d+)\.(\d+)", raw or "")
    return ".".join(m.groups()) if m else "2.5.0"


def _safe_name(name: str) -> str:
    name = (name or "").strip()
    if not name or len(name) > 120:
        raise BridgeError("El nombre del preset no puede estar vacío ni superar 120 caracteres.")
    if re.search(r'[\\/:*?"<>|\x00-\x1f]', name) or name in (".", ".."):
        raise BridgeError('El nombre del preset no puede contener \\ / : * ? " < > |')
    words = set(re.split(r"[^a-z]+", name.lower()))
    if words & DENIED_NAME_PARTS or name.lower().endswith((".conf", ".ini")):
        raise BridgeError("OrcaSlicer bloquea a los plugins los archivos cuyo nombre contiene "
                          "'config', 'conf', 'secret' o 'cert'. Elige otro nombre.")
    return name


def _data_dir() -> str:
    """data_dir() de OrcaSlicer, deducido de la carpeta privada del plugin."""
    p = os.path.abspath(STATE.storage)
    while True:
        parent = os.path.dirname(p)
        if os.path.basename(p) == "orca_plugins":
            return parent
        if parent == p:
            raise BridgeError("No puedo deducir la carpeta de datos de OrcaSlicer.")
        p = parent


# ─────────────────────────────────────────────────────────────────────────────
# Ejecutable de OrcaSlicer (para la CLI y para reabrir proyectos)
# ─────────────────────────────────────────────────────────────────────────────

def find_orca_executable(override: str = "") -> str:
    if override:
        return override
    try:
        if sys.platform.startswith("win"):
            import ctypes
            buf = ctypes.create_unicode_buffer(32768)
            if ctypes.windll.kernel32.GetModuleFileNameW(None, buf, 32768):
                return buf.value
        elif sys.platform == "darwin":
            import ctypes
            libc = ctypes.CDLL(None)
            size = ctypes.c_uint32(4096)
            buf = ctypes.create_string_buffer(4096)
            if libc._NSGetExecutablePath(buf, ctypes.byref(size)) == 0:
                return os.path.realpath(buf.value.decode("utf-8"))
        else:
            appimage = os.environ.get("APPIMAGE")
            if appimage and os.path.isfile(appimage):
                return appimage
            return os.path.realpath("/proc/self/exe")
    except Exception:  # noqa: BLE001
        traceback.print_exc()
    return ""


async def run_process(args: list[str], cwd: str, timeout: float) -> dict:
    """Lanza un proceso sin hilos auxiliares (sondeo con asyncio.sleep) y recoge su salida."""
    os.makedirs(cwd, exist_ok=True)
    out_path = os.path.join(cwd, "stdout.txt")
    err_path = os.path.join(cwd, "stderr.txt")
    flags = 0x08000000 if sys.platform.startswith("win") else 0  # CREATE_NO_WINDOW
    with open(out_path, "wb") as out, open(err_path, "wb") as err:
        try:
            proc = subprocess.Popen(args, cwd=cwd, stdout=out, stderr=err, stdin=subprocess.DEVNULL,
                                    creationflags=flags)
        except PermissionError as exc:
            raise BridgeError(
                "OrcaSlicer ha bloqueado lanzar su propio ejecutable. Acepta el permiso del plugin "
                f"'MCP Bridge' (Plugins → permisos) y repite la operación. Detalle: {exc}") from None
        except FileNotFoundError:
            raise BridgeError(f"No encuentro el ejecutable de OrcaSlicer: {args[0]}. "
                              "Indica la ruta en la configuración del plugin (orca_executable).") from None
        start = time.monotonic()
        while proc.poll() is None:
            if time.monotonic() - start > timeout:
                proc.kill()
                raise BridgeError(f"La CLI de OrcaSlicer superó el tiempo límite ({int(timeout)} s).")
            await asyncio.sleep(0.25)
    with open(out_path, "rb") as fh:
        stdout = fh.read().decode("utf-8", "replace")
    with open(err_path, "rb") as fh:
        stderr = fh.read().decode("utf-8", "replace")
    return {"returncode": proc.returncode, "stdout": stdout, "stderr": stderr,
            "seconds": round(time.monotonic() - start, 2)}


# ─────────────────────────────────────────────────────────────────────────────
# Editor de proyectos 3MF (formato de OrcaSlicer / Bambu Studio)
# ─────────────────────────────────────────────────────────────────────────────

_ATTR_RE = re.compile(r'([\w:.-]+)="([^"]*)"')
_ITEM_RE = re.compile(r"<item\b[^>]*?/>")
_VERTEX_RE = re.compile(r'<vertex\s+x="([^"]+)"\s+y="([^"]+)"\s+z="([^"]+)"')
MAIN_MODEL = "3D/3dmodel.model"
MODEL_SETTINGS = "Metadata/model_settings.config"
PROJECT_SETTINGS = "Metadata/project_settings.config"
_OBJECT_STRUCT_KEYS = {"name", "module"}


def _parse_attrs(tag: str) -> dict:
    return dict(_ATTR_RE.findall(tag))


def _fmt(x: float) -> str:
    s = "%.9g" % x
    return "0" if s in ("-0", "0") else s


class Affine:
    """Transformación 3MF: p' = A·p + t. Cadena 3MF = columnas de A y luego t."""

    def __init__(self, A=None, t=None):
        self.A = A or [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
        self.t = t or [0.0, 0.0, 0.0]

    @classmethod
    def parse(cls, s: str | None) -> "Affine":
        if not s:
            return cls()
        e = [float(v) for v in s.split()]
        if len(e) != 12:
            return cls()
        A = [[e[3 * c + r] for c in range(3)] for r in range(3)]
        return cls(A, e[9:12])

    def to_3mf(self) -> str:
        e = [self.A[r][c] for c in range(3) for r in range(3)] + list(self.t)
        return " ".join(_fmt(v) for v in e)

    def __matmul__(self, o: "Affine") -> "Affine":
        A = [[sum(self.A[r][k] * o.A[k][c] for k in range(3)) for c in range(3)] for r in range(3)]
        t = [sum(self.A[r][k] * o.t[k] for k in range(3)) + self.t[r] for r in range(3)]
        return Affine(A, t)

    @staticmethod
    def rotation(axis: str, deg: float) -> list:
        a = math.radians(deg)
        c, s = math.cos(a), math.sin(a)
        if axis == "x":
            return [[1, 0, 0], [0, c, -s], [0, s, c]]
        if axis == "y":
            return [[c, 0, s], [0, 1, 0], [-s, 0, c]]
        if axis == "z":
            return [[c, -s, 0], [s, c, 0], [0, 0, 1]]
        raise BridgeError(f"Eje no válido: {axis!r} (usa x, y o z)")

    def premul(self, M):
        self.A = [[sum(M[r][k] * self.A[k][c] for k in range(3)) for c in range(3)] for r in range(3)]

    def postmul(self, M):
        self.A = [[sum(self.A[r][k] * M[k][c] for k in range(3)) for c in range(3)] for r in range(3)]

    def scale_factors(self):
        return [math.sqrt(sum(self.A[r][c] ** 2 for r in range(3))) for c in range(3)]

    def apply(self, p) -> list:
        A, t = self.A, self.t
        return [A[r][0] * p[0] + A[r][1] * p[1] + A[r][2] * p[2] + t[r] for r in range(3)]

    def inverse(self) -> "Affine":
        a = self.A
        det = (a[0][0] * (a[1][1] * a[2][2] - a[1][2] * a[2][1]) - a[0][1] * (a[1][0] * a[2][2] - a[1][2] * a[2][0])
               + a[0][2] * (a[1][0] * a[2][1] - a[1][1] * a[2][0]))
        if abs(det) < 1e-12:
            raise BridgeError("Transformación no invertible (escala cero).")
        inv = [[(a[(c + 1) % 3][(r + 1) % 3] * a[(c + 2) % 3][(r + 2) % 3]
                 - a[(c + 1) % 3][(r + 2) % 3] * a[(c + 2) % 3][(r + 1) % 3]) / det for c in range(3)] for r in range(3)]
        t = [-sum(inv[r][k] * self.t[k] for k in range(3)) for r in range(3)]
        return Affine(inv, t)


# Listas por filamento de project_settings.config (OrcaSlicer 2.5.2: comparando un proyecto con 1 y 2 filamentos).
FILAMENT_VECTOR_KEYS = frozenset([
    "activate_air_filtration", "activate_air_filtration_during_print", "activate_air_filtration_on_completion",
    "activate_chamber_temp_control", "adaptive_pressure_advance", "adaptive_pressure_advance_bridges",
    "adaptive_pressure_advance_model", "adaptive_pressure_advance_overhangs", "additional_cooling_fan_speed",
    "additional_fan_full_speed_layer", "chamber_minimal_temperature", "chamber_temperature",
    "close_additional_fan_first_x_layers", "close_fan_the_first_x_layers", "complete_print_exhaust_fan_speed",
    "cool_plate_temp", "cool_plate_temp_initial_layer", "default_filament_colour", "dont_slow_down_outer_wall",
    "during_print_exhaust_fan_speed", "enable_overhang_bridge_fan", "enable_pressure_advance", "eng_plate_temp",
    "eng_plate_temp_initial_layer", "fan_cooling_layer_time", "fan_max_speed", "fan_min_speed",
    "filament_adaptive_volumetric_speed", "filament_adhesiveness_category", "filament_change_extrusion_role_gcode",
    "filament_change_length", "filament_change_length_nc", "filament_colour", "filament_colour_type",
    "filament_cooling_before_tower", "filament_cooling_final_speed", "filament_cooling_initial_speed",
    "filament_cooling_moves", "filament_cost", "filament_density", "filament_deretraction_speed",
    "filament_dev_ams_drying_ams_limitations", "filament_dev_ams_drying_heat_distortion_temperature",
    "filament_dev_ams_drying_temperature", "filament_dev_ams_drying_time",
    "filament_dev_chamber_drying_bed_temperature", "filament_dev_chamber_drying_time",
    "filament_dev_drying_cooling_temperature", "filament_dev_drying_softening_temperature", "filament_diameter",
    "filament_end_gcode", "filament_extruder_compatibility", "filament_extruder_variant", "filament_flow_ratio",
    "filament_flush_temp", "filament_flush_temp_fast", "filament_flush_volumetric_speed", "filament_ids",
    "filament_ironing_flow", "filament_ironing_inset", "filament_ironing_spacing", "filament_ironing_speed",
    "filament_is_mixed", "filament_is_support", "filament_loading_speed", "filament_loading_speed_start",
    "filament_long_retractions_when_cut", "filament_map", "filament_max_volumetric_speed",
    "filament_minimal_purge_on_wipe_tower", "filament_mixed_components", "filament_mixed_gradient",
    "filament_mixed_gradient_curve", "filament_mixed_gradient_per_part", "filament_mixed_gradient_range",
    "filament_mixed_sublayer_ratios", "filament_multi_colour", "filament_multitool_ramming",
    "filament_multitool_ramming_flow", "filament_multitool_ramming_volume", "filament_notes",
    "filament_nozzle_map", "filament_pre_cooling_temperature", "filament_pre_cooling_temperature_nc",
    "filament_preheat_temperature_delta", "filament_prime_volume", "filament_prime_volume_nc",
    "filament_printable", "filament_ramming_parameters", "filament_ramming_travel_time",
    "filament_ramming_travel_time_nc", "filament_ramming_volumetric_speed", "filament_ramming_volumetric_speed_nc",
    "filament_retract_after_wipe", "filament_retract_before_wipe", "filament_retract_length_nc",
    "filament_retract_length_toolchange", "filament_retract_lift_above", "filament_retract_lift_below",
    "filament_retract_lift_enforce", "filament_retract_restart_extra", "filament_retract_restart_extra_toolchange",
    "filament_retract_when_changing_layer", "filament_retraction_distances_when_cut", "filament_retraction_length",
    "filament_retraction_minimum_travel", "filament_retraction_speed", "filament_settings_id", "filament_shrink",
    "filament_shrinkage_compensation_z", "filament_soluble", "filament_stamping_distance",
    "filament_stamping_loading_speed", "filament_start_gcode", "filament_toolchange_delay",
    "filament_tower_interface_pre_extrusion_dist", "filament_tower_interface_pre_extrusion_length",
    "filament_tower_interface_print_temp", "filament_tower_interface_purge_volume", "filament_tower_ironing_area",
    "filament_type", "filament_unloading_speed", "filament_unloading_speed_start", "filament_vendor",
    "filament_volume_map", "filament_wipe", "filament_wipe_distance", "filament_z_hop", "filament_z_hop_types",
    "first_x_layer_fan_speed", "full_fan_speed_layer", "hot_plate_temp", "hot_plate_temp_initial_layer",
    "idle_temperature", "initial_layer_fan_speed", "internal_bridge_fan_speed", "ironing_fan_speed",
    "long_retractions_when_ec", "nozzle_temperature", "nozzle_temperature_initial_layer",
    "nozzle_temperature_range_high", "nozzle_temperature_range_low", "overhang_fan_speed",
    "overhang_fan_threshold", "pellet_flow_coefficient", "pressure_advance", "reduce_fan_stop_start_freq",
    "required_nozzle_HRC", "retraction_distances_when_ec", "slow_down_for_layer_cooling", "slow_down_layer_time",
    "slow_down_min_speed", "supertack_plate_temp", "supertack_plate_temp_initial_layer",
    "support_material_interface_fan_speed", "temperature_vitrification", "textured_cool_plate_temp",
    "textured_cool_plate_temp_initial_layer", "textured_plate_temp", "textured_plate_temp_initial_layer",
    "volumetric_speed_coefficients",
])


def _subdivide(V, T, max_edge, limit=400_000, codes=None, active=None):
    """Divide aristas más largas que max_edge por su punto medio (malla conforme: misma forma, sin grietas).
    active(p0, p1, p2) -> bool limita la subdivisión a una zona; codes (pintura por triángulo) se heredan."""
    V = [list(v) for v in V]
    codes = list(codes) if codes is not None else [{} for _ in T]
    m2 = max_edge * max_edge

    def d2(a, b):
        pa, pb = V[a], V[b]
        return (pa[0] - pb[0]) ** 2 + (pa[1] - pb[1]) ** 2 + (pa[2] - pb[2]) ** 2
    while True:
        mids = {}
        for a, b, c in T:
            if active is not None and not active(V[a], V[b], V[c]):
                continue
            for u, w in ((a, b), (b, c), (c, a)):
                k = (u, w) if u < w else (w, u)
                if k not in mids and d2(u, w) > m2:
                    mids[k] = None
        if not mids:
            return V, T, codes
        if len(T) + 3 * len(mids) > limit:
            raise BridgeError(f"La subdivisión superaría {limit} triángulos; usa formas más pequeñas o max_edge mayor.")
        for k in mids:
            pa, pb = V[k[0]], V[k[1]]
            mids[k] = len(V)
            V.append([(pa[i] + pb[i]) / 2 for i in range(3)])
        out, out_codes = [], []
        for (a, b, c), code in zip(T, codes):
            def mid(u, w):
                return mids.get((u, w) if u < w else (w, u))
            ab, bc, ca = mid(a, b), mid(b, c), mid(c, a)
            n = (ab is not None) + (bc is not None) + (ca is not None)
            if n == 0:
                new = [(a, b, c)]
            elif n == 3:
                new = [(a, ab, ca), (ab, b, bc), (ca, bc, c), (ab, bc, ca)]
            else:
                for (p, q, r, pq, qr, rp) in ((a, b, c, ab, bc, ca), (b, c, a, bc, ca, ab), (c, a, b, ca, ab, bc)):
                    if pq is not None and (n == 1 or qr is not None):
                        break
                new = [(p, pq, r), (pq, q, r)] if n == 1 else [(pq, q, qr), (p, pq, qr), (p, qr, r)]
            out += new
            # Un código de hoja (sin división interna) vale para los hijos; uno con división se pierde.
            if isinstance(code, dict):
                keep = {k: v for k, v in code.items() if not k.startswith("paint") or _is_leaf_code(v)}
                out_codes += [dict(keep) for _ in new]
                continue
            keep = code if _is_leaf_code(code) else ""
            out_codes += [keep] * len(new)
        T, codes = out, out_codes


def _is_leaf_code(code: str) -> bool:
    if not code:
        return True
    last = int(code[-1], 16)
    return (last & 0b11) == 0  # sin lados divididos


def _surface_dots(V, T, spacing, seed=1):
    """Centros repartidos por la superficie con separación mínima (muestreo de disco de Poisson por rechazo)."""
    import random
    rng = random.Random(seed)
    areas, total = [], 0.0
    for a, b, c in T:
        pa, pb, pc = V[a], V[b], V[c]
        u = [pb[i] - pa[i] for i in range(3)]
        w = [pc[i] - pa[i] for i in range(3)]
        cr = (u[1] * w[2] - u[2] * w[1], u[2] * w[0] - u[0] * w[2], u[0] * w[1] - u[1] * w[0])
        total += 0.5 * math.sqrt(cr[0] ** 2 + cr[1] ** 2 + cr[2] ** 2)
        areas.append(total)
    import bisect
    cell = spacing / math.sqrt(3)
    grid, dots = {}, []
    tries = int(total / (spacing * spacing) * 30) + 100
    s2 = spacing * spacing
    for _ in range(tries):
        i = bisect.bisect_left(areas, rng.random() * total)
        a, b, c = T[min(i, len(T) - 1)]
        r1, r2 = rng.random(), rng.random()
        if r1 + r2 > 1:
            r1, r2 = 1 - r1, 1 - r2
        p = [V[a][k] + r1 * (V[b][k] - V[a][k]) + r2 * (V[c][k] - V[a][k]) for k in range(3)]
        g = tuple(int(math.floor(p[k] / cell)) for k in range(3))
        ok = True
        for dx in (-2, -1, 0, 1, 2):
            for dy in (-2, -1, 0, 1, 2):
                for dz in (-2, -1, 0, 1, 2):
                    for q in grid.get((g[0] + dx, g[1] + dy, g[2] + dz), ()):
                        if (p[0] - q[0]) ** 2 + (p[1] - q[1]) ** 2 + (p[2] - q[2]) ** 2 < s2:
                            ok = False
                            break
                    if not ok:
                        break
                if not ok:
                    break
            if not ok:
                break
        if ok:
            grid.setdefault(g, []).append(p)
            dots.append(p)
    return dots


def _paint_near(V, T, dots, radius):
    cell = radius
    grid = {}
    for p in dots:
        grid.setdefault(tuple(int(math.floor(p[k] / cell)) for k in range(3)), []).append(p)
    r2 = radius * radius
    out = []
    for a, b, c in T:
        cen = [(V[a][k] + V[b][k] + V[c][k]) / 3 for k in range(3)]
        g = tuple(int(math.floor(cen[k] / cell)) for k in range(3))
        hit = False
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    for q in grid.get((g[0] + dx, g[1] + dy, g[2] + dz), ()):
                        if (cen[0] - q[0]) ** 2 + (cen[1] - q[1]) ** 2 + (cen[2] - q[2]) ** 2 <= r2:
                            hit = True
                            break
                    if hit:
                        break
                if hit:
                    break
            if hit:
                break
        out.append(hit)
    return out


# ── utilidades de malla para las herramientas de modelo ─────────────────────
PAINT_LAYERS = {"color": "paint_color", "supports": "paint_supports", "seam": "paint_seam",
                "fuzzy_skin": "paint_fuzzy_skin"}
PART_TYPES = {"part": ("normal_part", "Pieza"), "negative": ("negative_part", "Volumen negativo"),
              "modifier": ("modifier_part", "Modificador"), "support_enforcer": ("support_enforcer", "Forzar soporte"),
              "support_blocker": ("support_blocker", "Bloquear soporte")}
_PART_TYPE_NAMES = {v[0]: k for k, v in PART_TYPES.items()}
LAYER_RANGES_FILE = "Metadata/layer_config_ranges.xml"
LAYER_PROFILE_FILE = "Metadata/layer_heights_profile.txt"
CUSTOM_GCODE_FILE = "Metadata/custom_gcode_per_layer.xml"
BRIM_EARS_FILE = "Metadata/brim_ear_points.txt"


def _uuid() -> str:
    import uuid
    return str(uuid.uuid4())


def _xml_text(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _mesh_xml(V, T, attrs=None) -> str:
    ind = "     "
    verts = "\n".join(f'{ind}<vertex x="{_fmt(x)}" y="{_fmt(y)}" z="{_fmt(z)}"/>' for x, y, z in V)
    rows = []
    for i, (a, b, c) in enumerate(T):
        extra = "".join(f' {k}="{v}"' for k, v in (attrs[i].items() if attrs else ()) if v != "")
        rows.append(f'{ind}<triangle v1="{a}" v2="{b}" v3="{c}"{extra}/>')
    return f"\n    <vertices>\n{verts}\n    </vertices>\n    <triangles>\n" + "\n".join(rows) + "\n    </triangles>\n   "


def _describe_code(layer: str, code: str) -> str:
    if layer == "color":
        if len(code) == 1:
            v = int(code, 16)
            return f"filamento {v >> 2}" if v & 3 == 0 else f"subdividido {code}"
        if len(code) == 2 and code[1] == "C":
            return f"filamento {int(code[0], 16) + 3}"
        return f"subdividido {code[:8]}"
    names = {"4": "forzar" if layer != "fuzzy_skin" else "piel difusa", "8": "bloquear"}
    return names.get(code, f"subdividido {code[:8]}")


def _bbox_near(lo, hi, margin):
    def near(q):
        for k in range(3):
            a = min(p[k] for p in q) - margin
            b = max(p[k] for p in q) + margin
            if b < lo[k] or a > hi[k]:
                return False
        return True
    return near


def _tri_normal(p0, p1, p2):
    u = [p1[k] - p0[k] for k in range(3)]
    v = [p2[k] - p0[k] for k in range(3)]
    n = [u[1] * v[2] - u[2] * v[1], u[2] * v[0] - u[0] * v[2], u[0] * v[1] - u[1] * v[0]]
    ln = math.sqrt(n[0] * n[0] + n[1] * n[1] + n[2] * n[2]) or 1.0
    return [n[0] / ln, n[1] / ln, n[2] / ln]


def _edge_adjacency(T):
    edges = {}
    for i, (a, b, c) in enumerate(T):
        for u, w in ((a, b), (b, c), (c, a)):
            edges.setdefault((u, w) if u < w else (w, u), []).append(i)
    adj = [[] for _ in T]
    for tris in edges.values():
        for i in tris:
            for j in tris:
                if i != j:
                    adj[i].append(j)
    return adj


def _smart_fill(W, T, seed, angle_deg):
    """Relleno inteligente: desde el triángulo más cercano a `seed`, se extiende por triángulos vecinos cuya
    normal no se desvía más de angle_deg de la del vecino (como el 'Relleno inteligente' de OrcaSlicer)."""
    if not T:
        return set()
    best, bd = 0, math.inf
    for i, (a, b, c) in enumerate(T):
        cen = [(W[a][k] + W[b][k] + W[c][k]) / 3 for k in range(3)]
        d = sum((cen[k] - seed[k]) ** 2 for k in range(3))
        if d < bd:
            best, bd = i, d
    normals = [_tri_normal(W[a], W[b], W[c]) for a, b, c in T]
    adj = _edge_adjacency(T)
    cos_lim = math.cos(math.radians(angle_deg))
    sel, stack = {best}, [best]
    while stack:
        i = stack.pop()
        for j in adj[i]:
            if j not in sel and sum(normals[i][k] * normals[j][k] for k in range(3)) >= cos_lim:
                sel.add(j)
                stack.append(j)
    return sel


def _connected_components(T):
    parent = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for a, b, c in T:
        ra, rb, rc = find(a), find(b), find(c)
        parent[rb] = ra
        parent[find(rc)] = ra
    groups = {}
    for i, (a, _b, _c) in enumerate(T):
        groups.setdefault(find(a), []).append(i)
    return list(groups.values())


def _submesh(V, T, attrs, tri_ids):
    remap, V2, T2, A2 = {}, [], [], []
    for i in tri_ids:
        tri = []
        for v in T[i]:
            if v not in remap:
                remap[v] = len(V2)
                V2.append(list(V[v]))
            tri.append(remap[v])
        T2.append(tuple(tri))
        A2.append(dict(attrs[i]) if attrs else {})
    return V2, T2, A2


def _convex_hull(pts):
    pts = sorted(set((round(x, 4), round(y, 4)) for x, y in pts))
    if len(pts) < 3:
        return pts

    def cross(o, a, b):
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
    lower, upper = [], []
    for p in pts:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    for p in reversed(pts):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    return lower[:-1] + upper[:-1]


def _frange(a, b, step):
    x = a
    while x <= b + 1e-9:
        yield x
        x += step


def _rotation_xyz(rot: dict):
    """Matriz de rotación a partir de {x, y, z} en grados (se aplica X, luego Y, luego Z)."""
    R = [[1.0, 0, 0], [0, 1.0, 0], [0, 0, 1.0]]
    for axis in ("x", "y", "z"):
        if rot.get(axis):
            M = Affine.rotation(axis, float(rot[axis]))
            R = [[sum(M[r][k] * R[k][c] for k in range(3)) for c in range(3)] for r in range(3)]
    return R


# ── primitivas (mallas cerradas, normales hacia fuera, centradas en el origen) ──
def _prim_box(sx, sy, sz):
    x, y, z = sx / 2, sy / 2, sz / 2
    V = [[-x, -y, -z], [x, -y, -z], [x, y, -z], [-x, y, -z], [-x, -y, z], [x, -y, z], [x, y, z], [-x, y, z]]
    T = [(0, 2, 1), (0, 3, 2), (4, 5, 6), (4, 6, 7), (0, 1, 5), (0, 5, 4),
         (1, 2, 6), (1, 6, 5), (2, 3, 7), (2, 7, 6), (3, 0, 4), (3, 4, 7)]
    return V, T


def _prim_cylinder(r_bottom, r_top, h, n=64):
    V, T = [], []
    z0, z1 = -h / 2, h / 2
    for i in range(n):
        a = 2 * math.pi * i / n
        V.append([r_bottom * math.cos(a), r_bottom * math.sin(a), z0])
    for i in range(n):
        a = 2 * math.pi * i / n
        V.append([r_top * math.cos(a), r_top * math.sin(a), z1])
    cb, ct = len(V), len(V) + 1
    V += [[0, 0, z0], [0, 0, z1]]
    for i in range(n):
        j = (i + 1) % n
        T.append((cb, j, i))
        if r_top > 1e-9:
            T += [(i, j, n + j), (i, n + j, n + i)]
            T.append((ct, n + i, n + j))
        else:
            T.append((i, j, ct))
    if r_top <= 1e-9:  # cono: sin anillo superior
        keep = list(range(n)) + [cb, ct]
        remap = {v: k for k, v in enumerate(keep)}
        V = [V[v] for v in keep]
        T = [tuple(remap[v] for v in t) for t in T]
    return V, T


def _prim_sphere(r, nu=48, nv=24):
    V = [[0, 0, -r]]
    for j in range(1, nv):
        th = math.pi * j / nv - math.pi / 2
        for i in range(nu):
            ph = 2 * math.pi * i / nu
            V.append([r * math.cos(th) * math.cos(ph), r * math.cos(th) * math.sin(ph), r * math.sin(th)])
    V.append([0, 0, r])
    top = len(V) - 1
    T = []

    def ring(j, i):
        return 1 + (j - 1) * nu + (i % nu)
    for i in range(nu):
        T.append((0, ring(1, i + 1), ring(1, i)))
        T.append((top, ring(nv - 1, i), ring(nv - 1, i + 1)))
    for j in range(1, nv - 1):
        for i in range(nu):
            a, b, c, d = ring(j, i), ring(j, i + 1), ring(j + 1, i + 1), ring(j + 1, i)
            T += [(a, b, c), (a, c, d)]
    return V, T


def _prim_torus(R, r, nu=64, nv=24):
    V, T = [], []
    for i in range(nu):
        u = 2 * math.pi * i / nu
        for j in range(nv):
            v = 2 * math.pi * j / nv
            V.append([(R + r * math.cos(v)) * math.cos(u), (R + r * math.cos(v)) * math.sin(u), r * math.sin(v)])
    for i in range(nu):
        for j in range(nv):
            a = i * nv + j
            b = ((i + 1) % nu) * nv + j
            c = ((i + 1) % nu) * nv + (j + 1) % nv
            d = i * nv + (j + 1) % nv
            T += [(a, b, c), (a, c, d)]
    return V, T


def _read_stl(path):
    with open(path, "rb") as f:
        data = f.read()
    import struct
    tris = []
    if len(data) >= 84:
        n = struct.unpack_from("<I", data, 80)[0]
        if 84 + 50 * n == len(data):
            for k in range(n):
                vals = struct.unpack_from("<12f", data, 84 + 50 * k)
                tris.append((vals[3:6], vals[6:9], vals[9:12]))
    if not tris:
        nums = re.findall(rb"vertex\s+(\S+)\s+(\S+)\s+(\S+)", data)
        pts = [tuple(float(v) for v in p) for p in nums]
        tris = [tuple(pts[i:i + 3]) for i in range(0, len(pts) - 2, 3)]
    index, V, T = {}, [], []
    for tri in tris:
        ids = []
        for p in tri:
            key = (round(p[0], 5), round(p[1], 5), round(p[2], 5))
            if key not in index:
                index[key] = len(V)
                V.append(list(key))
            ids.append(index[key])
        if len(set(ids)) == 3:
            T.append(tuple(ids))
    return V, T


def _read_obj(path):
    V, T = [], []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            p = line.split()
            if not p:
                continue
            if p[0] == "v":
                V.append([float(p[1]), float(p[2]), float(p[3])])
            elif p[0] == "f":
                ids = [int(s.split("/")[0]) for s in p[1:]]
                ids = [i - 1 if i > 0 else len(V) + i for i in ids]
                for k in range(1, len(ids) - 1):
                    T.append((ids[0], ids[k], ids[k + 1]))
    return V, T


def _read_3mf_mesh(path):
    """Une las mallas de todos los objetos de un 3MF (p. ej. el convertido por la CLI de OrcaSlicer)."""
    p = Project3MF(path)
    V, T = [], []
    for obj in p.objects():
        inst = Affine.parse(obj["instances"][0]["transform"])
        for pi, comp in enumerate(p._components(obj["object_id"]) or []):
            _pp, _t, _m, v, t, _a = p._read_part_mesh(obj, pi)
            W = inst @ Affine.parse(_parse_attrs(comp).get("transform"))
            base = len(V)
            V += [W.apply(q) for q in v]
            T += [(a + base, b + base, c + base) for a, b, c in t]
        if not p._components(obj["object_id"]):
            m = re.search(r'<object\b[^>]*\bid="%s"[^>]*>(.*?)</object>' % re.escape(obj["object_id"]), p.model, re.S)
            v = [[float(x), float(y), float(z)] for x, y, z in _VERTEX_RE.findall(m.group(1))]
            t = [tuple(int(q) for q in tt) for tt in
                 re.findall(r'<triangle\s+v1="(\d+)"\s+v2="(\d+)"\s+v3="(\d+)"', m.group(1))]
            base = len(V)
            V += [inst.apply(q) for q in v]
            T += [(a + base, b + base, c + base) for a, b, c in t]
    return V, T


def _mesh_for_op(op: dict):
    """Malla de una operación: shape (box, cylinder, cone, sphere, torus) o file (STL/OBJ/3MF)."""
    if op.get("_mesh_file") or op.get("file"):
        path = op.get("_mesh_file") or op["file"]
        ext = os.path.splitext(path)[1].lower()
        try:
            V, T = (_read_stl(path) if ext == ".stl" else _read_obj(path) if ext == ".obj"
                    else _read_3mf_mesh(path) if ext == ".3mf" else (None, None))
        except OSError as e:
            raise BridgeError(f"No puedo leer {path}: {e}") from None
        if V is None:
            raise BridgeError(f"Formato no admitido directamente: {ext} (usa STL, OBJ o 3MF; STEP se convierte con la CLI).")
        if not T:
            raise BridgeError(f"{path} no contiene triángulos.")
        s = float(op.get("scale", 1.0))
        lo = [min(p[k] for p in V) for k in range(3)]
        hi = [max(p[k] for p in V) for k in range(3)]
        c = [(lo[k] + hi[k]) / 2 for k in range(3)]
        return [[(p[k] - c[k]) * s for k in range(3)] for p in V], T
    shape = str(op.get("shape", "box")).lower()
    size = op.get("size")
    if shape in ("box", "cube"):
        if size is None:
            size = [20, 20, 20]
        sx, sy, sz = (float(size),) * 3 if not isinstance(size, (list, tuple)) else [float(v) for v in size]
        return _prim_box(sx, sy, sz)
    if shape in ("cylinder", "disc", "disk"):
        r = float(op.get("radius", (size[0] / 2) if isinstance(size, (list, tuple)) else 10))
        h = float(op.get("height", size[2] if isinstance(size, (list, tuple)) else (2 if shape != "cylinder" else 20)))
        return _prim_cylinder(r, r, h, int(op.get("segments", 64)))
    if shape == "cone":
        r = float(op.get("radius", 10))
        return _prim_cylinder(r, float(op.get("top_radius", 0)), float(op.get("height", 20)), int(op.get("segments", 64)))
    if shape == "sphere":
        return _prim_sphere(float(op.get("radius", 10)))
    if shape == "torus":
        return _prim_torus(float(op.get("radius", 15)), float(op.get("tube_radius", 4)))
    raise BridgeError(f"shape desconocida: {shape!r} (box, cylinder, disc, cone, sphere, torus) o usa file.")



# ── corte por plano (la CLI de OrcaSlicer 2.5.2 no expone --cut) ────────────
def _plane_split(V, T, attrs, n, d, eps=1e-7):
    """Divide una malla por el plano n·p = d. Devuelve (arriba, abajo) como (V, T, attrs, segmentos_del_borde)."""
    s = [sum(n[k] * p[k] for k in range(3)) - d for p in V]
    s = [x if abs(x) > eps else eps for x in s]          # ningún vértice exactamente en el plano
    out = {True: ([], [], [], []), False: ([], [], [], [])}
    vmap = {True: {}, False: {}}
    cut_pts = {}

    def vid(side, key, p):
        m = vmap[side]
        if key not in m:
            m[key] = len(out[side][0])
            out[side][0].append(list(p))
        return m[key]

    def cut(a, b):
        k = (a, b) if a < b else (b, a)
        if k not in cut_pts:
            t = s[a] / (s[a] - s[b])
            cut_pts[k] = [V[a][q] + t * (V[b][q] - V[a][q]) for q in range(3)]
        return ("e",) + k, cut_pts[k]

    for i, (a, b, c) in enumerate(T):
        at = attrs[i] if attrs else {}
        leaf = {k: v for k, v in at.items() if not k.startswith("paint") or _is_leaf_code(v)}
        sides = [s[a] > 0, s[b] > 0, s[c] > 0]
        if sides[0] == sides[1] == sides[2]:
            side = sides[0]
            o = out[side]
            o[1].append(tuple(vid(side, ("v", v), V[v]) for v in (a, b, c)))
            o[2].append(dict(at))
            continue
        # rota para que el vértice solitario sea el primero, conservando la orientación
        tri = [a, b, c]
        for r in range(3):
            if sides[r] != sides[(r + 1) % 3] and sides[r] != sides[(r + 2) % 3]:
                tri = tri[r:] + tri[:r]
                break
        p, q, r_ = tri
        lone = s[p] > 0
        kpq, ppq = cut(p, q)
        kpr, ppr = cut(p, r_)
        # lado del vértice solitario: un triángulo
        o = out[lone]
        A = vid(lone, ("v", p), V[p])
        B = vid(lone, kpq, ppq)
        C = vid(lone, kpr, ppr)
        o[1].append((A, B, C))
        o[2].append(dict(leaf))
        o[3].append((B, C))                               # arista sobre el plano (orientada)
        # el otro lado: un cuadrilátero
        other = not lone
        o2 = out[other]
        B2 = vid(other, kpq, ppq)
        C2 = vid(other, kpr, ppr)
        Q = vid(other, ("v", q), V[q])
        R = vid(other, ("v", r_), V[r_])
        o2[1].extend([(B2, Q, R), (B2, R, C2)])
        o2[2].extend([dict(leaf), dict(leaf)])
        o2[3].append((C2, B2))
    return out[True], out[False]


def _loops_from_edges(edges):
    nxt = {}
    for a, b in edges:
        nxt.setdefault(a, []).append(b)
    loops, used = [], set()
    for start in list(nxt):
        for first in nxt[start]:
            if (start, first) in used:
                continue
            loop, cur, prev = [start], first, start
            used.add((start, first))
            ok = False
            for _ in range(len(edges) + 1):
                if cur == start:
                    ok = True
                    break
                loop.append(cur)
                cand = [x for x in nxt.get(cur, []) if (cur, x) not in used]
                if not cand:
                    break
                used.add((cur, cand[0]))
                cur = cand[0]
            if ok and len(loop) >= 3:
                loops.append(loop)
    return loops


def _area2(poly):
    return sum(poly[i][0] * poly[(i + 1) % len(poly)][1] - poly[(i + 1) % len(poly)][0] * poly[i][1]
               for i in range(len(poly))) / 2


def _point_in_poly(pt, poly):
    x, y = pt
    inside = False
    for i in range(len(poly)):
        x1, y1 = poly[i]
        x2, y2 = poly[(i + 1) % len(poly)]
        if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / (y2 - y1 + 1e-300) + x1:
            inside = not inside
    return inside


def _ear_clip(idx, P):
    """Triangula un polígono simple (índices idx, coordenadas 2D P) en sentido antihorario."""
    idx = list(idx)
    tris = []

    def convex(a, b, c):
        return (P[b][0] - P[a][0]) * (P[c][1] - P[a][1]) - (P[b][1] - P[a][1]) * (P[c][0] - P[a][0]) > 1e-14

    def inside(p, a, b, c):
        def sgn(p1, p2, p3):
            return (p1[0] - p3[0]) * (p2[1] - p3[1]) - (p2[0] - p3[0]) * (p1[1] - p3[1])
        d1, d2, d3 = sgn(p, P[a], P[b]), sgn(p, P[b], P[c]), sgn(p, P[c], P[a])
        return not ((d1 < 0 or d2 < 0 or d3 < 0) and (d1 > 0 or d2 > 0 or d3 > 0))
    guard = 0
    while len(idx) > 3 and guard < 10 * len(idx) * len(idx) + 100:
        guard += 1
        n = len(idx)
        for i in range(n):
            a, b, c = idx[i - 1], idx[i], idx[(i + 1) % n]
            if not convex(a, b, c):
                continue
            if any(inside(P[v], a, b, c) for v in idx if v not in (a, b, c) and P[v] not in (P[a], P[b], P[c])):
                continue
            tris.append((a, b, c))
            idx.pop(i)
            break
        else:  # polígono degenerado: abanico para no dejar agujeros
            tris += [(idx[0], idx[k], idx[k + 1]) for k in range(1, len(idx) - 1)]
            return tris
    if len(idx) == 3:
        tris.append(tuple(idx))
    return tris


def _cap(loops, P2):
    """Triangula las tapas: contornos exteriores (antihorario) con sus agujeros (horario) unidos por puentes."""
    if not loops:
        return []
    # El sentido de los contornos depende de qué lado se tapa: el contorno de mayor área marca el "exterior".
    big = max(loops, key=lambda lp: abs(_area2([P2[v] for v in lp])))
    if _area2([P2[v] for v in big]) < 0:
        P2 = {v: (x, -y) for v, (x, y) in P2.items()}   # espejo: el ear clipping trabaja en antihorario
    outers, holes = [], []
    for lp in loops:
        poly = [P2[v] for v in lp]
        (outers if _area2(poly) > 0 else holes).append(lp)
    tris = []
    for outer in outers:
        poly = [P2[v] for v in outer]
        mine = [h for h in holes if _point_in_poly(P2[h[0]], poly)]
        ring = list(outer)
        for h in sorted(mine, key=lambda h: -max(P2[v][0] for v in h)):
            hi = max(range(len(h)), key=lambda k: P2[h[k]][0])
            hv = h[hi]
            # vértice del contorno más cercano a la derecha del agujero (puente simple)
            best = min(range(len(ring)), key=lambda k: (P2[ring[k]][0] < P2[hv][0],
                                                        (P2[ring[k]][0] - P2[hv][0]) ** 2 + (P2[ring[k]][1] - P2[hv][1]) ** 2))
            hole_seq = h[hi:] + h[:hi] + [hv]
            ring = ring[:best + 1] + hole_seq + [ring[best]] + ring[best + 1:]
        tris += _ear_clip(ring, P2)
    return tris


def _close_cut(part, n, cap_attrs=None):
    """Añade la tapa plana a una mitad cortada (arriba: tapa mirando a -n; abajo: a +n)."""
    V, T, A, edges = part
    if not edges:
        return V, T, A
    # base ortonormal del plano
    ax = [1.0, 0, 0] if abs(n[0]) < 0.9 else [0, 1.0, 0]
    u = [n[1] * ax[2] - n[2] * ax[1], n[2] * ax[0] - n[0] * ax[2], n[0] * ax[1] - n[1] * ax[0]]
    lu = math.sqrt(sum(x * x for x in u))
    u = [x / lu for x in u]
    w = [n[1] * u[2] - n[2] * u[1], n[2] * u[0] - n[0] * u[2], n[0] * u[1] - n[1] * u[0]]
    P2 = {}
    for a, b in edges:
        for v in (a, b):
            if v not in P2:
                P2[v] = (sum(V[v][k] * u[k] for k in range(3)), sum(V[v][k] * w[k] for k in range(3)))
    # Las aristas del borde recorren el agujero de la malla; la tapa las recorre al revés.
    loops = _loops_from_edges([(b, a) for a, b in edges])
    tris = _cap(loops, P2)
    T = list(T) + [tuple(t) for t in tris]
    A = list(A) + [{} for _ in tris]
    return V, T, A


def _signed_volume(V, T):
    return sum((V[a][0] * (V[b][1] * V[c][2] - V[b][2] * V[c][1]) - V[a][1] * (V[b][0] * V[c][2] - V[b][2] * V[c][0])
                + V[a][2] * (V[b][0] * V[c][1] - V[b][1] * V[c][0])) for a, b, c in T) / 6.0


def _rotation_to(a, b):
    """Rotación que lleva el vector unitario a sobre b (Rodrigues)."""
    v = [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]
    c = sum(a[k] * b[k] for k in range(3))
    if c > 1 - 1e-12:
        return [[1.0, 0, 0], [0, 1.0, 0], [0, 0, 1.0]]
    if c < -1 + 1e-12:  # 180°: gira sobre un eje perpendicular cualquiera
        ax = [1.0, 0, 0] if abs(a[0]) < 0.9 else [0, 1.0, 0]
        u = [a[1] * ax[2] - a[2] * ax[1], a[2] * ax[0] - a[0] * ax[2], a[0] * ax[1] - a[1] * ax[0]]
        lu = math.sqrt(sum(x * x for x in u))
        u = [x / lu for x in u]
        return [[2 * u[r] * u[k] - (1 if r == k else 0) for k in range(3)] for r in range(3)]
    K = [[0, -v[2], v[1]], [v[2], 0, -v[0]], [-v[1], v[0], 0]]
    K2 = [[sum(K[r][k] * K[k][q] for k in range(3)) for q in range(3)] for r in range(3)]
    f = 1 / (1 + c)
    return [[(1 if r == q else 0) + K[r][q] + K2[r][q] * f for q in range(3)] for r in range(3)]


CORE_OPS = {
    "paint": "paint", "paint_dots": "paint_dots", "clear_paint": "clear_paint",
    "add_filament": "add_filament", "set_flush_volumes": "set_flush_volumes", "set_filament_color": "set_filament_color",
    "add_part": "add_part", "remove_part": "remove_part", "set_part_type": "set_part_type", "rename_part": "rename_part",
    "move_part": "transform_part", "rotate_part": "transform_part", "scale_part": "transform_part",
    "add_object": "add_object", "add_instance": "add_instance", "remove_instance": "remove_instance",
    "split_to_parts": "split_to_parts", "split_to_objects": "split_to_objects",
    "set_layer_range": "set_layer_range", "clear_layer_ranges": "clear_layer_ranges",
    "set_variable_layer_height": "set_variable_layer_height",
    "clear_variable_layer_height": "clear_variable_layer_height",
    "add_layer_gcode": "add_layer_gcode", "clear_layer_gcode": "clear_layer_gcode",
    "set_brim_ears": "set_brim_ears", "set_plate": "set_plate", "set_project_info": "set_project_info",
    "cut": "cut",
}


class Project3MF:
    """Carga, consulta y modifica un proyecto 3MF de OrcaSlicer conservando todo lo demás."""

    def __init__(self, path: str):
        self.path = path
        with zipfile.ZipFile(path) as z:
            self.names = z.namelist()
            self.data = {n: z.read(n) for n in self.names}
        if MAIN_MODEL not in self.data:
            raise BridgeError("El archivo no es un proyecto 3MF válido (falta 3D/3dmodel.model).")
        self.model = self.data[MAIN_MODEL].decode("utf-8")
        self.settings = (ET.fromstring(self.data[MODEL_SETTINGS])
                         if MODEL_SETTINGS in self.data else ET.Element("config"))
        self._mesh_cache: dict = {}
        self.dirty_model = self.dirty_settings = self.dirty_project = False
        self.project: dict | None = None
        if PROJECT_SETTINGS in self.data:
            try:
                self.project = json.loads(self.data[PROJECT_SETTINGS].decode("utf-8-sig") or "{}")
            except ValueError:
                self.project = None

    # ── índice de objetos ───────────────────────────────────────────────
    def _items(self):
        build = self.model[self.model.find("<build"):]
        base = self.model.find("<build")
        return [(m.start() + base, m.end() + base, m.group(0)) for m in _ITEM_RE.finditer(build)]

    def objects(self) -> list[dict]:
        order, by_id = [], {}
        for _s, _e, tag in self._items():
            a = _parse_attrs(tag)
            oid = a.get("objectid")
            if oid not in by_id:
                by_id[oid] = {"object_id": oid, "instances": []}
                order.append(oid)
            tr = Affine.parse(a.get("transform"))
            by_id[oid]["instances"].append({
                "instance": len(by_id[oid]["instances"]),
                "position": [round(v, 4) for v in tr.t],
                "scale": [round(v, 5) for v in tr.scale_factors()],
                "printable": a.get("printable", "1") != "0",
                "transform": a.get("transform", ""),
            })
        out = []
        for idx, oid in enumerate(order):
            obj = by_id[oid]
            node = self._settings_object(oid)
            obj["index"] = idx
            obj["name"] = self._meta(node, "name") if node is not None else ""
            obj["settings"] = {} if node is None else {
                m.get("key"): m.get("value") for m in node.findall("metadata")
                if m.get("key") not in _OBJECT_STRUCT_KEYS}
            obj["parts"] = [] if node is None else [{
                "part": i, "id": p.get("id"), "subtype": p.get("subtype"),
                "name": self._meta(p, "name"),
                "settings": {m.get("key"): m.get("value") for m in p.findall("metadata")
                             if m.get("key") not in ("name", "matrix", "source_file", "source_object_id",
                                                     "source_volume_id", "source_offset_x", "source_offset_y",
                                                     "source_offset_z", "mesh_shared")},
            } for i, p in enumerate(node.findall("part"))]
            out.append(obj)
        return out

    def _settings_object(self, oid):
        for o in self.settings.findall("object"):
            if o.get("id") == str(oid):
                return o
        return None

    @staticmethod
    def _meta(node, key):
        for m in node.findall("metadata"):
            if m.get("key") == key:
                return m.get("value")
        return None

    @staticmethod
    def _set_meta(node, key, value):
        for m in node.findall("metadata"):
            if m.get("key") == key:
                m.set("value", str(value))
                return
        el = ET.Element("metadata", {"key": key, "value": str(value)})
        # metadatos antes de los <part>, como los escribe OrcaSlicer
        idx = sum(1 for c in node if c.tag == "metadata")
        node.insert(idx, el)

    def resolve(self, target) -> dict:
        objs = self.objects()
        if isinstance(target, int) or (isinstance(target, str) and target.isdigit()):
            i = int(target)
            if 0 <= i < len(objs):
                return objs[i]
            raise BridgeError(f"No hay objeto con índice {i}. Hay {len(objs)} objetos (0..{len(objs) - 1}).")
        matches = [o for o in objs if o["name"] == target]
        if len(matches) == 1:
            return matches[0]
        if not matches:
            raise BridgeError(f"No hay ningún objeto llamado {target!r}. Objetos: {[o['name'] for o in objs]}")
        raise BridgeError(f"Hay {len(matches)} objetos llamados {target!r}; usa el índice.")

    # ── geometría ───────────────────────────────────────────────────────
    def _vertices(self, path: str, oid: str):
        key = (path, oid)
        if key in self._mesh_cache:
            return self._mesh_cache[key]
        text = self.model if path == MAIN_MODEL else self.data[path].decode("utf-8")
        m = re.search(r'<object\b[^>]*\bid="%s"[^>]*>' % re.escape(oid), text)
        if not m:
            raise BridgeError(f"Objeto {oid} no encontrado en {path}")
        end = text.find("</object>", m.end())
        body = text[m.end():end]
        result = []
        if "<mesh" in body:
            result.append((Affine(), [(float(x), float(y), float(z)) for x, y, z in _VERTEX_RE.findall(body)]))
        for comp in re.findall(r"<component\b[^>]*/>", body):
            a = _parse_attrs(comp)
            sub_path = a.get("p:path", "").lstrip("/") or path
            tr = Affine.parse(a.get("transform"))
            for sub_tr, verts in self._vertices(sub_path, a["objectid"]):
                result.append((tr @ sub_tr, verts))
        self._mesh_cache[key] = result
        return result

    def world_bbox(self, oid: str, item_tr: Affine):
        lo, hi = [math.inf] * 3, [-math.inf] * 3
        for comp_tr, verts in self._vertices(MAIN_MODEL, oid):
            T = item_tr @ comp_tr
            A, t = T.A, T.t
            for x, y, z in verts:
                for r in range(3):
                    v = A[r][0] * x + A[r][1] * y + A[r][2] * z + t[r]
                    if v < lo[r]:
                        lo[r] = v
                    if v > hi[r]:
                        hi[r] = v
        if lo[0] is math.inf:
            raise BridgeError("El objeto no tiene geometría.")
        return lo, hi

    # ── escritura de transformaciones ──────────────────────────────────
    def _instance_tags(self, oid: str):
        return [(s, e, tag) for s, e, tag in self._items() if _parse_attrs(tag).get("objectid") == oid]

    def _replace_item_attr(self, oid: str, instance: int | None, attr: str, fn):
        tags = self._instance_tags(oid)
        if instance is not None and not 0 <= instance < len(tags):
            raise BridgeError(f"El objeto no tiene la instancia {instance} (tiene {len(tags)}).")
        for i, (s, e, tag) in reversed(list(enumerate(tags))):
            if instance is not None and i != instance:
                continue
            old = _parse_attrs(tag).get(attr)
            new = fn(old, i)
            if f' {attr}="' in tag:
                new_tag = re.sub(r'(\s%s=")[^"]*(")' % re.escape(attr), lambda m: m.group(1) + new + m.group(2), tag)
            else:
                new_tag = tag[:-2].rstrip() + f' {attr}="{new}"/>'
            self.model = self.model[:s] + new_tag + self.model[e:]
            if attr == "transform":
                self._sync_assemble(oid, i, new)
        self.dirty_model = True

    def _sync_assemble(self, oid, instance, transform):
        asm = self.settings.find("assemble")
        if asm is None:
            return
        for it in asm.findall("assemble_item"):
            if it.get("object_id") == str(oid) and it.get("instance_id") == str(instance):
                it.set("transform", transform)
                self.dirty_settings = True

    def transform_instances(self, target, instance, mutate, drop: bool):
        obj = self.resolve(target)
        oid = obj["object_id"]

        def fn(old, _i):
            tr = Affine.parse(old)
            mutate(tr)
            if drop:
                lo, _hi = self.world_bbox(oid, tr)
                tr.t[2] -= lo[2]
            return tr.to_3mf()
        self._replace_item_attr(oid, instance, "transform", fn)
        return obj

    # ── operaciones de alto nivel ──────────────────────────────────────
    def apply(self, op: dict) -> str:
        kind = op.get("op")
        target = op.get("object", 0)
        inst = op.get("instance")
        inst = None if inst is None else int(inst)
        if kind == "translate":
            d = [float(op.get(k, 0.0)) for k in ("x", "y", "z")]
            obj = self.transform_instances(target, inst, lambda tr: [tr.t.__setitem__(r, tr.t[r] + d[r]) for r in range(3)], False)
            return f"{obj['name']}: desplazado {d} mm"
        if kind == "set_position":
            def mut(tr):
                for r, k in enumerate(("x", "y", "z")):
                    if op.get(k) is not None:
                        tr.t[r] = float(op[k])
            obj = self.transform_instances(target, inst, mut, op.get("z") is None and bool(op.get("drop_to_bed", False)))
            return f"{obj['name']}: colocado en {[op.get(k) for k in ('x', 'y', 'z')]}"
        if kind == "rotate":
            axis = str(op.get("axis", "z")).lower()
            deg = float(op["degrees"])
            R = Affine.rotation(axis, deg)
            obj = self.transform_instances(target, inst, lambda tr: tr.premul(R), bool(op.get("drop_to_bed", True)))
            return f"{obj['name']}: rotado {deg}° en {axis.upper()}"
        if kind == "scale":
            f = op.get("factor", 1.0)
            fx, fy, fz = (float(f),) * 3 if not isinstance(f, (list, tuple)) else [float(v) for v in f]
            if min(fx, fy, fz) <= 0:
                raise BridgeError("El factor de escala debe ser positivo.")
            obj = self.transform_instances(target, inst, lambda tr: tr.postmul([[fx, 0, 0], [0, fy, 0], [0, 0, fz]]),
                                           bool(op.get("drop_to_bed", True)))
            return f"{obj['name']}: escalado ×{[fx, fy, fz]}"
        if kind == "mirror":
            axis = str(op.get("axis", "x")).lower()
            M = [[-1 if (axis == a and a == b) else (1 if a == b else 0) for b in "xyz"] for a in "xyz"]
            obj = self.transform_instances(target, inst, lambda tr: tr.postmul(M), bool(op.get("drop_to_bed", True)))
            return f"{obj['name']}: reflejado en {axis.upper()}"
        if kind == "drop_to_bed":
            obj = self.transform_instances(target, inst, lambda tr: None, True)
            return f"{obj['name']}: apoyado en la cama"
        if kind == "set_printable":
            obj = self.resolve(target)
            val = "1" if op.get("printable", True) else "0"
            self._replace_item_attr(obj["object_id"], inst, "printable", lambda _o, _i: val)
            return f"{obj['name']}: imprimible={val == '1'}"
        if kind == "rename":
            obj = self.resolve(target)
            node = self._require_node(obj)
            self._set_meta(node, "name", op["name"])
            self.dirty_settings = True
            return f"objeto {obj['index']} renombrado a {op['name']!r}"
        if kind in ("set_setting", "remove_setting", "set_extruder"):
            obj = self.resolve(target)
            node = self._require_node(obj)
            if op.get("part") is not None:
                parts = node.findall("part")
                p = int(op["part"])
                if not 0 <= p < len(parts):
                    raise BridgeError(f"{obj['name']} no tiene la pieza {p} (tiene {len(parts)}).")
                node = parts[p]
            key = "extruder" if kind == "set_extruder" else str(op["key"])
            if key in ("name", "matrix"):
                raise BridgeError("Esa clave no es un ajuste.")
            if kind == "remove_setting":
                for m in list(node.findall("metadata")):
                    if m.get("key") == key:
                        node.remove(m)
            else:
                value = op.get("extruder") if kind == "set_extruder" else op.get("value")
                self._set_meta(node, key, serialize_value(value))
            self.dirty_settings = True
            where = obj["name"] + (f" / pieza {op['part']}" if op.get("part") is not None else "")
            return f"{where}: {kind} {key}"
        if kind == "delete":
            obj = self.resolve(target)
            self._delete_object(obj["object_id"])
            return f"objeto {obj['name']!r} eliminado"
        if kind in CORE_OPS:
            return getattr(self, CORE_OPS[kind])(op)
        if kind in EXTRA_3MF_OPS:
            return EXTRA_3MF_OPS[kind](self, op)
        if kind == "set_project_setting":
            if self.project is None:
                raise BridgeError("Este proyecto no contiene Metadata/project_settings.config.")
            key, val = str(op["key"]), op.get("value")
            cur = self.project.get(key)
            if isinstance(cur, list):
                vals = val if isinstance(val, list) else [val] * max(1, len(cur))
                self.project[key] = [serialize_value(v) for v in vals]
            else:
                self.project[key] = serialize_value(val, cur if isinstance(cur, str) else None)
            self.dirty_project = True
            return f"ajuste de proyecto {op['key']} = {op.get('value')!r}"
        raise BridgeError(f"Operación desconocida: {kind!r}. Extra: {sorted(EXTRA_3MF_OPS)}")

    # ── pintura multicolor (atributo paint_color de OrcaSlicer) ─────────
    @staticmethod
    def paint_code(extruder: int) -> str:
        """Código de TriangleSelector para un triángulo sin dividir pintado con un extrusor."""
        e = int(extruder)
        if not 1 <= e <= 17:
            raise BridgeError("El extrusor a pintar debe estar entre 1 y 17.")
        return format(e << 2, "X") if e < 3 else format(e - 3, "X") + "C"

    def _part_mesh_ref(self, obj, part: int):
        """(ruta del archivo, id de objeto) de la malla de una pieza."""
        m = re.search(r'<object\b[^>]*\bid="%s"[^>]*>(.*?)</object>' % re.escape(obj["object_id"]), self.model, re.S)
        comps = re.findall(r"<component\b[^>]*/>", m.group(1)) if m else []
        if not comps:
            raise BridgeError("Este objeto guarda la malla en línea; la pintura solo admite objetos por componentes.")
        if not 0 <= part < len(comps):
            raise BridgeError(f"{obj['name']} no tiene la pieza {part} (tiene {len(comps)}).")
        a = _parse_attrs(comps[part])
        return a.get("p:path", "").lstrip("/") or MAIN_MODEL, a["objectid"]

    # ── mallas por pieza (conserva todas las capas de pintura) ──────────
    def _read_part_mesh(self, obj, part):
        path, oid = self._part_mesh_ref(obj, part)
        text = self.data[path].decode("utf-8")
        m = re.search(r'(<object\b[^>]*\bid="%s"[^>]*>\s*<mesh>)(.*?)(</mesh>)' % re.escape(oid), text, re.S)
        if not m:
            raise BridgeError("No encuentro la malla de la pieza.")
        body = m.group(2)
        V = [[float(x), float(y), float(z)] for x, y, z in _VERTEX_RE.findall(body)]
        T, attrs = [], []
        for tag in re.findall(r"<triangle\b[^>]*/>", body):
            a = _parse_attrs(tag)
            T.append((int(a.pop("v1")), int(a.pop("v2")), int(a.pop("v3"))))
            attrs.append(a)
        return path, text, m, V, T, attrs

    def _write_part_mesh(self, path, text, m, V, T, attrs):
        text = text[:m.start(2)] + _mesh_xml(V, T, attrs) + text[m.end(2):]
        self.data[path] = text.encode("utf-8")
        if path == MAIN_MODEL:
            self.model = text
        self._mesh_cache.clear()

    def _world_transform(self, obj, part, instance=0):
        """Transformación pieza → cama (instancia ∘ componente)."""
        comp = self._components(obj["object_id"])[part]
        return Affine.parse(obj["instances"][instance]["transform"]) @ Affine.parse(_parse_attrs(comp).get("transform"))

    @staticmethod
    def _paint_value(layer: str, sh: dict, default_ext):
        """Código TriangleSelector a escribir ('' = borrar) para la capa de pintura."""
        if sh.get("erase"):
            return ""
        if layer == "color":
            ext = sh.get("extruder", default_ext)
            return "" if ext in (0, None) else Project3MF.paint_code(ext)
        if layer in ("supports", "seam"):
            mode = str(sh.get("mode", "enforce")).lower()
            if mode not in ("enforce", "block"):
                raise BridgeError("mode debe ser 'enforce' (forzar) o 'block' (bloquear).")
            return "4" if mode == "enforce" else "8"
        return "4"  # fuzzy_skin: un solo estado

    def _selectors(self, shapes, layer, default_ext, V, T, W, max_edge):
        """Prepara las formas: (selección por triángulo, zona de subdivisión, código)."""
        prepared = []
        for sh in shapes:
            kind = sh.get("shape", "box")
            code = self._paint_value(layer, sh, default_ext)
            nf = sh.get("normal")
            if nf is not None:
                ln = math.sqrt(sum(v * v for v in nf)) or 1.0
                nf = [v / ln for v in nf]
            min_dot = float(sh.get("min_dot", 0.5))
            near = None   # f(q0, q1, q2) -> bool: el triángulo (en coordenadas de la forma) toca la zona
            pick = None   # f(i, cen, nrm, pts) -> bool
            if kind == "box":
                lo = [min(a, b) for a, b in zip(sh["min"], sh["max"])]
                hi = [max(a, b) for a, b in zip(sh["min"], sh["max"])]
                pick = (lambda lo, hi: lambda i, c, n, q: all(lo[k] <= c[k] <= hi[k] for k in range(3)))(lo, hi)
                near = _bbox_near(lo, hi, max_edge)
            elif kind == "sphere":
                c0, r = [float(v) for v in sh["center"]], float(sh["radius"])
                pick = (lambda c0, r2: lambda i, c, n, q: sum((c[k] - c0[k]) ** 2 for k in range(3)) <= r2)(c0, r * r)
                near = _bbox_near([c0[k] - r for k in range(3)], [c0[k] + r for k in range(3)], max_edge)
            elif kind == "capsule":
                a_, b_, r = [float(v) for v in sh["a"]], [float(v) for v in sh["b"]], float(sh["radius"])
                ab = [b_[k] - a_[k] for k in range(3)]
                L2 = sum(v * v for v in ab) or 1e-12

                def pick(i, c, n, q, a_=a_, ab=ab, L2=L2, r2=r * r):
                    t = max(0.0, min(1.0, sum((c[k] - a_[k]) * ab[k] for k in range(3)) / L2))
                    return sum((c[k] - a_[k] - t * ab[k]) ** 2 for k in range(3)) <= r2
                near = _bbox_near([min(a_[k], b_[k]) - r for k in range(3)],
                                  [max(a_[k], b_[k]) + r for k in range(3)], max_edge)
            elif kind == "cylinder":
                # cilindro vertical (eje Z) — útil para bandas y círculos vistos desde arriba
                c0, r = [float(v) for v in sh["center"]], float(sh["radius"])
                z0, z1 = float(sh.get("z_min", -1e9)), float(sh.get("z_max", 1e9))
                pick = (lambda c0, r2, z0, z1: lambda i, c, n, q: z0 <= c[2] <= z1 and
                        (c[0] - c0[0]) ** 2 + (c[1] - c0[1]) ** 2 <= r2)(c0, r * r, z0, z1)
                near = _bbox_near([c0[0] - r, c0[1] - r, z0], [c0[0] + r, c0[1] + r, z1], max_edge)
            elif kind == "height":
                z0, z1 = float(sh.get("z_min", -1e9)), float(sh.get("z_max", 1e9))
                pick = (lambda z0, z1: lambda i, c, n, q: z0 <= c[2] <= z1)(z0, z1)
                near = _bbox_near([-1e9, -1e9, z0], [1e9, 1e9, z1], max_edge)
            elif kind == "half_space":
                p0 = [float(v) for v in sh["point"]]
                nn = [float(v) for v in sh["normal"]]
                ln = math.sqrt(sum(v * v for v in nn)) or 1.0
                nn = [v / ln for v in nn]

                def sd(p, p0=p0, nn=nn):
                    return sum((p[k] - p0[k]) * nn[k] for k in range(3))
                pick = (lambda sd: lambda i, c, n, q: sd(c) >= 0)(sd)
                near = (lambda sd: lambda q: min(sd(p) for p in q) <= max_edge and max(sd(p) for p in q) >= -max_edge)(sd)
            elif kind == "overhang":
                ang = math.radians(float(sh.get("angle", 45)))
                limit = -math.sin(ang)
                zmin = min(p[2] for p in W) if W else 0.0
                pick = (lambda limit, zmin: lambda i, c, n, q: n[2] < limit and
                        max(p[2] for p in q) > zmin + 0.05)(limit, zmin)
            elif kind == "smart_fill":
                sel = _smart_fill(W, T, [float(v) for v in sh["seed"]], float(sh.get("angle", 30)))
                pick = (lambda sel: lambda i, c, n, q: i in sel)(sel)
            elif kind == "all":
                pick = lambda i, c, n, q: True  # noqa: E731
            else:
                raise BridgeError(f"Forma desconocida: {kind!r}. Usa box, sphere, capsule, cylinder, height, "
                                  "half_space, overhang, smart_fill o all.")
            prepared.append((pick, near, code, nf, min_dot))
        return prepared

    def paint(self, op: dict) -> str:
        """Pinta zonas de una pieza en cualquier capa: color (MMU), supports, seam o fuzzy_skin."""
        obj = self.resolve(op.get("object", 0))
        part = int(op.get("part", 0))
        layer = str(op.get("layer", "color"))
        if layer not in PAINT_LAYERS:
            raise BridgeError(f"layer debe ser una de {sorted(PAINT_LAYERS)}")
        attr = PAINT_LAYERS[layer]
        shapes = op.get("shapes") or ([{k: v for k, v in op.items() if k not in ("op", "object", "part", "layer")}]
                                      if op.get("shape") else [])
        if not shapes:
            raise BridgeError("Indica shapes: lista de formas a pintar (o shape en la propia operación).")
        X = self._world_transform(obj, part) if op.get("frame", "world") == "world" else Affine()
        A, t = X.A, X.t

        def w(p):
            return [A[r][0] * p[0] + A[r][1] * p[1] + A[r][2] * p[2] + t[r] for r in range(3)]
        path, text, m, V, T, attrs = self._read_part_mesh(obj, part)
        n0 = len(T)
        max_edge = float(op.get("max_edge", 0.4))
        prepared = self._selectors(shapes, layer, op.get("extruder", 2), V, T, [w(p) for p in V], max_edge)
        nears = [nr for _p, nr, _c, _n, _d in prepared if nr is not None]
        if nears and op.get("subdivide", True):
            def active(p0, p1, p2):
                q = [w(p0), w(p1), w(p2)]
                return any(nr(q) for nr in nears)
            V, T, attrs = _subdivide(V, T, max_edge, int(op.get("max_triangles", 400_000)), attrs, active)
            if any(sh.get("shape") == "smart_fill" for sh in shapes):
                prepared = self._selectors(shapes, layer, op.get("extruder", 2), V, T, [w(p) for p in V], max_edge)
        Wv = [w(p) for p in V]
        counts = [0] * len(prepared)
        for i, (a, b, c) in enumerate(T):
            q = (Wv[a], Wv[b], Wv[c])
            cen = [(q[0][k] + q[1][k] + q[2][k]) / 3 for k in range(3)]
            nrm = _tri_normal(*q)
            for j, (pick, _near, code, nf, min_dot) in enumerate(prepared):
                if nf is not None and sum(nrm[k] * nf[k] for k in range(3)) < min_dot:
                    continue
                if not pick(i, cen, nrm, q):
                    continue
                if code:
                    attrs[i][attr] = code
                else:
                    attrs[i].pop(attr, None)
                counts[j] += 1
        self._write_part_mesh(path, text, m, V, T, attrs)
        if layer == "supports" and op.get("enable_setting", True) and any(
                c == "4" for c in (pr[2] for pr in prepared)):
            # OrcaSlicer solo genera soporte en lo pintado si el soporte está activo; en modo manual solo ahí.
            node = self._require_node(obj)
            proj = self.project or {}
            if self._meta(node, "enable_support") is None and str(proj.get("enable_support", "0")) in ("0", "false"):
                st = str(proj.get("support_type", "normal(auto)"))
                self._set_meta(node, "enable_support", "1")
                self._set_meta(node, "support_type", "tree(manual)" if st.startswith("tree") else "normal(manual)")
                self.dirty_settings = True
        if layer == "fuzzy_skin" and op.get("enable_setting", True):
            node = self._require_node(obj)
            if self._meta(node, "fuzzy_skin") in (None, "none"):
                self._set_meta(node, "fuzzy_skin", "painted")  # OrcaSlicer: piel difusa solo en lo pintado
                self.dirty_settings = True
        detail = ", ".join(f"forma {j + 1}: {c} triángulos" for j, c in enumerate(counts))
        return f"{obj['name']}: capa {layer} pintada ({detail}); malla {n0} → {len(T)} triángulos"

    def paint_dots(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        part = int(op.get("part", 0))
        layer = str(op.get("layer", "color"))
        attr = PAINT_LAYERS.get(layer) or "paint_color"
        radius = float(op.get("radius", 2.5))
        spacing = float(op.get("spacing", radius * 3))
        max_edge = float(op.get("max_edge", max(radius / 3.0, 0.25)))
        code = self._paint_value(layer, op, op.get("extruder", 2))
        if radius <= 0 or spacing <= radius:
            raise BridgeError("Hace falta radius > 0 y spacing > radius.")
        path, text, m, V, T, attrs = self._read_part_mesh(obj, part)
        n_orig = len(T)
        V, T, attrs = _subdivide(V, T, max_edge, int(op.get("max_triangles", 400_000)), attrs)
        dots = _surface_dots(V, T, spacing, int(op.get("seed", 1)))
        painted = _paint_near(V, T, dots, radius)
        for i, hit in enumerate(painted):
            if hit:
                attrs[i][attr] = code
        self._write_part_mesh(path, text, m, V, T, attrs)
        return (f"{obj['name']}: {len(dots)} topos (radio {radius} mm, separación {spacing} mm) en la capa {layer}"
                f"{' con el extrusor ' + str(op.get('extruder', 2)) if layer == 'color' else ''}; "
                f"malla {n_orig} → {len(T)} triángulos, {sum(painted)} pintados")

    def clear_paint(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        layer = str(op.get("layer", "color"))
        keys = set(PAINT_LAYERS.values()) if layer == "all" else {PAINT_LAYERS.get(layer, "")}
        if "" in keys:
            raise BridgeError(f"layer debe ser una de {sorted(PAINT_LAYERS)} o 'all'.")
        parts = [int(op["part"])] if op.get("part") is not None else range(len(self._components(obj["object_id"])))
        n = 0
        for part in parts:
            path, text, m, V, T, attrs = self._read_part_mesh(obj, part)
            for a in attrs:
                for k in keys:
                    if a.pop(k, None) is not None:
                        n += 1
            self._write_part_mesh(path, text, m, V, T, attrs)
        return f"{obj['name']}: pintura '{layer}' borrada en {n} triángulos"

    # ── ids, archivos y componentes ────────────────────────────────────
    def _components(self, oid: str) -> list[str]:
        m = re.search(r'<object\b[^>]*\bid="%s"[^>]*>(.*?)</object>' % re.escape(str(oid)), self.model, re.S)
        return re.findall(r"<component\b[^>]*/>", m.group(1)) if m else []

    def _new_id(self) -> int:
        ids = [0]
        for n, d in self.data.items():
            if n.endswith(".model"):
                txt = self.model if n == MAIN_MODEL else d.decode("utf-8", "replace")
                ids += [int(x) for x in re.findall(r'<object\b[^>]*\bid="(\d+)"', txt)]
        self._id_floor = max(max(ids), getattr(self, "_id_floor", 0)) + 1
        return self._id_floor

    def _model_index(self, oid: str) -> int:
        """Posición del objeto en Model::objects de OrcaSlicer (orden de <object> en el modelo principal)."""
        res = self.model[self.model.find("<resources"):self.model.find("</resources>")]
        order = re.findall(r'<object\b[^>]*\bid="(\d+)"', res)
        return order.index(str(oid))

    def _new_object_file(self, name: str, oid: int) -> str:
        safe = re.sub(r"[^\w.-]+", "_", name)[:40] or "objeto"
        path = f"3D/Objects/{safe}_{oid}.model"
        self.data[path] = (
            '<?xml version="1.0" encoding="UTF-8"?>\n<model unit="millimeter" xml:lang="en-US" '
            'xmlns="http://schemas.microsoft.com/3dmanufacturing/core/2015/02" '
            'xmlns:BambuStudio="http://schemas.bambulab.com/package/2021" '
            'xmlns:p="http://schemas.microsoft.com/3dmanufacturing/production/2015/06" requiredextensions="p">\n'
            ' <metadata name="BambuStudio:3mfVersion">1</metadata>\n <resources>\n </resources>\n <build/>\n</model>\n'
        ).encode("utf-8")
        self.names.append(path)
        rels = "3D/_rels/3dmodel.model.rels"
        txt = self.data.get(rels, b"").decode("utf-8") or (
            '<?xml version="1.0" encoding="UTF-8"?>\n<Relationships '
            'xmlns="http://schemas.openxmlformats.org/package/2006/relationships">\n</Relationships>')
        if rels not in self.names:
            self.names.append(rels)
        n = len(re.findall(r"<Relationship\b", txt)) + 1
        txt = txt.replace("</Relationships>", f' <Relationship Target="/{path}" Id="rel-{n}" '
                          'Type="http://schemas.microsoft.com/3dmanufacturing/2013/01/3dmodel"/>\n</Relationships>')
        self.data[rels] = txt.encode("utf-8")
        return path

    def _append_mesh_object(self, path: str, sub_id: int, V, T, attrs=None):
        text = self.data[path].decode("utf-8")
        obj = (f'  <object id="{sub_id}" p:UUID="{_uuid()}" type="model">\n   <mesh>'
               f'{_mesh_xml(V, T, attrs)}</mesh>\n  </object>\n')
        i = text.rfind("</resources>")
        text = text[:i] + obj + " " + text[i:]
        self.data[path] = text.encode("utf-8")
        self._mesh_cache.clear()

    def _remove_mesh_object(self, path: str, sub_id: str):
        text = self.data[path].decode("utf-8")
        m = re.search(r'\s*<object\b[^>]*\bid="%s"[^>]*>.*?</object>' % re.escape(str(sub_id)), text, re.S)
        if m:
            text = text[:m.start()] + text[m.end():]
            self.data[path] = text.encode("utf-8")
        self._mesh_cache.clear()

    def _add_component(self, oid: str, path: str, sub_id: int, tr: Affine):
        comp = (f'<component p:path="/{path}" objectid="{sub_id}" p:UUID="{_uuid()}" '
                f'transform="{tr.to_3mf()}"/>')
        m = re.search(r'(<object\b[^>]*\bid="%s"[^>]*>.*?)(\s*</components>)' % re.escape(str(oid)), self.model, re.S)
        if not m:
            raise BridgeError("El objeto no está guardado por componentes.")
        self.model = self.model[:m.end(1)] + "\n    " + comp + self.model[m.end(1):]
        self.dirty_model = True
        self._mesh_cache.clear()

    def _remove_component(self, oid: str, index: int) -> dict:
        m = re.search(r'<object\b[^>]*\bid="%s"[^>]*>.*?</object>' % re.escape(str(oid)), self.model, re.S)
        comps = list(re.finditer(r"\s*<component\b[^>]*/>", m.group(0)))
        c = comps[index]
        s, e = m.start() + c.start(), m.start() + c.end()
        attrs = _parse_attrs(c.group(0))
        self.model = self.model[:s] + self.model[e:]
        self.dirty_model = True
        self._mesh_cache.clear()
        return attrs

    def _set_component_transform(self, oid: str, index: int, tr: Affine):
        m = re.search(r'<object\b[^>]*\bid="%s"[^>]*>.*?</object>' % re.escape(str(oid)), self.model, re.S)
        c = list(re.finditer(r"<component\b[^>]*/>", m.group(0)))[index]
        tag = c.group(0)
        new = (re.sub(r'(\stransform=")[^"]*(")', lambda mm: mm.group(1) + tr.to_3mf() + mm.group(2), tag)
               if ' transform="' in tag else tag[:-2].rstrip() + f' transform="{tr.to_3mf()}"/>')
        s = m.start() + c.start()
        self.model = self.model[:s] + new + self.model[s + len(tag):]
        self.dirty_model = True
        self._mesh_cache.clear()

    def _part_nodes(self, obj):
        return self._require_node(obj).findall("part")

    def _identify_ids(self):
        return [int(self._meta(mi, "identify_id") or 0) for pl in self.settings.findall("plate")
                for mi in pl.findall("model_instance")]

    def _add_plate_instance(self, oid, instance: int, plate: int = 1):
        plates = self.settings.findall("plate")
        if not plates:
            return
        pl = plates[max(0, min(len(plates), plate) - 1)]
        mi = ET.SubElement(pl, "model_instance")
        for k, v in (("object_id", oid), ("instance_id", instance), ("identify_id", max(self._identify_ids() + [0]) + 1)):
            ET.SubElement(mi, "metadata", {"key": k, "value": str(v)})
        self.dirty_settings = True

    def _add_assemble(self, oid, instance: int, tr: Affine):
        asm = self.settings.find("assemble")
        if asm is None:
            asm = ET.SubElement(self.settings, "assemble")
        ET.SubElement(asm, "assemble_item", {"object_id": str(oid), "instance_id": str(instance),
                                            "transform": tr.to_3mf(), "offset": "0 0 0"})
        self.dirty_settings = True

    # ── piezas: modificadores, negativos, forzadores/bloqueadores de soporte ──
    def _object_frame_from_world(self, obj, center, rotate=None):
        """Transformación de componente que coloca una malla (centrada en el origen) en `center` (cama)."""
        inst = Affine.parse(obj["instances"][0]["transform"])
        inv = inst.inverse()
        R = _rotation_xyz(rotate or {})
        comp = Affine([[sum(inv.A[r][k] * R[k][c] for k in range(3)) for c in range(3)] for r in range(3)],
                      inv.apply(center))
        return comp

    def _object_world_bbox(self, obj, instance=0):
        return self.world_bbox(obj["object_id"], Affine.parse(obj["instances"][instance]["transform"]))

    def add_part(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        ptype = str(op.get("type", "modifier"))
        if ptype not in PART_TYPES:
            raise BridgeError(f"type debe ser uno de {sorted(PART_TYPES)}")
        V, T = _mesh_for_op(op)
        lo, hi = self._object_world_bbox(obj)
        center = op.get("center") or op.get("position") or [(lo[k] + hi[k]) / 2 for k in range(3)]
        center = [float(v) for v in center]
        comps = self._components(obj["object_id"])
        if not comps:
            raise BridgeError("Este objeto guarda la malla en línea; no admite piezas nuevas.")
        path = _parse_attrs(comps[0]).get("p:path", "").lstrip("/")
        sub = self._new_id()
        self._append_mesh_object(path, sub, V, T)
        self._add_component(obj["object_id"], path, sub, self._object_frame_from_world(obj, center, op.get("rotate")))
        node = self._require_node(obj)
        name = op.get("name") or f"{PART_TYPES[ptype][1]} {len(node.findall('part')) + 1}"
        part = ET.SubElement(node, "part", {"id": str(sub), "subtype": PART_TYPES[ptype][0]})
        ET.SubElement(part, "metadata", {"key": "name", "value": str(name)})
        ET.SubElement(part, "metadata", {"key": "matrix", "value": "1 0 0 0 0 1 0 0 0 0 1 0 0 0 0 1"})
        if op.get("extruder") is not None:
            ET.SubElement(part, "metadata", {"key": "extruder", "value": str(int(op["extruder"]))})
        for k, v in (op.get("settings") or {}).items():
            ET.SubElement(part, "metadata", {"key": str(k), "value": serialize_value(v)})
        self.dirty_settings = True
        return (f"{obj['name']}: añadida pieza {len(node.findall('part')) - 1} '{name}' ({ptype}, "
                f"{len(T)} triángulos) en {[round(v, 2) for v in center]}")

    def remove_part(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        idx = int(op["part"])
        parts = self._part_nodes(obj)
        comps = self._components(obj["object_id"])
        if not 0 <= idx < len(comps):
            raise BridgeError(f"{obj['name']} no tiene la pieza {idx} (tiene {len(comps)}).")
        normal = [p for p in parts if p.get("subtype", "normal_part") == "normal_part"]
        if idx < len(parts) and parts[idx].get("subtype", "normal_part") == "normal_part" and len(normal) <= 1:
            raise BridgeError("No se puede quitar la única pieza sólida; borra el objeto entero.")
        a = self._remove_component(obj["object_id"], idx)
        self._remove_mesh_object(a.get("p:path", "").lstrip("/") or MAIN_MODEL, a["objectid"])
        name = ""
        if idx < len(parts):
            name = self._meta(parts[idx], "name") or ""
            self._require_node(obj).remove(parts[idx])
            self.dirty_settings = True
        return f"{obj['name']}: pieza {idx} '{name}' eliminada"

    def set_part_type(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        ptype = str(op["type"])
        if ptype not in PART_TYPES:
            raise BridgeError(f"type debe ser uno de {sorted(PART_TYPES)}")
        parts = self._part_nodes(obj)
        idx = int(op["part"])
        if not 0 <= idx < len(parts):
            raise BridgeError(f"{obj['name']} no tiene la pieza {idx}.")
        parts[idx].set("subtype", PART_TYPES[ptype][0])
        self.dirty_settings = True
        return f"{obj['name']}: pieza {idx} ahora es {ptype}"

    def rename_part(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        parts = self._part_nodes(obj)
        idx = int(op["part"])
        if not 0 <= idx < len(parts):
            raise BridgeError(f"{obj['name']} no tiene la pieza {idx}.")
        self._set_meta(parts[idx], "name", op["name"])
        self.dirty_settings = True
        return f"{obj['name']}: pieza {idx} renombrada a {op['name']!r}"

    def transform_part(self, op: dict) -> str:
        """move_part / rotate_part / scale_part: en ejes de la cama, alrededor del centro de la pieza."""
        obj = self.resolve(op.get("object", 0))
        idx = int(op["part"])
        comps = self._components(obj["object_id"])
        if not 0 <= idx < len(comps):
            raise BridgeError(f"{obj['name']} no tiene la pieza {idx}.")
        inst = Affine.parse(obj["instances"][0]["transform"])
        comp = Affine.parse(_parse_attrs(comps[idx]).get("transform"))
        world = inst @ comp
        # centro de la pieza en la cama
        a = _parse_attrs(comps[idx])
        verts = self._vertices(a.get("p:path", "").lstrip("/") or MAIN_MODEL, a["objectid"])[0][1]
        pts = [world.apply(v) for v in verts]
        c = [(min(p[k] for p in pts) + max(p[k] for p in pts)) / 2 for k in range(3)]
        kind = op["op"]
        if kind == "move_part":
            d = [float(op.get(k, 0.0)) for k in ("x", "y", "z")]
            M = Affine(None, d)
        else:
            if kind == "rotate_part":
                L = Affine.rotation(str(op.get("axis", "z")).lower(), float(op["degrees"]))
            else:
                f = op.get("factor", 1.0)
                fx, fy, fz = (float(f),) * 3 if not isinstance(f, (list, tuple)) else [float(v) for v in f]
                L = [[fx, 0, 0], [0, fy, 0], [0, 0, fz]]
            M = Affine(None, c) @ Affine(L) @ Affine(None, [-v for v in c])
        new_comp = inst.inverse() @ M @ world
        self._set_component_transform(obj["object_id"], idx, new_comp)
        return f"{obj['name']}: pieza {idx} {kind.replace('_part', '')} aplicado"

    # ── objetos e instancias ────────────────────────────────────────────
    def _bed(self):
        P = self.project or {}
        pts = P.get("printable_area") or ["0x0", "256x0", "256x256", "0x256"]
        xy = [[float(v) for v in s.split("x")] for s in pts]
        return [min(p[0] for p in xy), min(p[1] for p in xy)], [max(p[0] for p in xy), max(p[1] for p in xy)]

    def _free_spot(self, size_xy):
        (bx0, by0), (bx1, by1) = self._bed()
        boxes = []
        for o in self.objects():
            for i in range(len(o["instances"])):
                lo, hi = self._object_world_bbox(o, i)
                boxes.append((lo, hi))
        sx, sy = size_xy[0] / 2 + 3, size_xy[1] / 2 + 3
        step = max(5.0, min(size_xy) / 2)
        cx, cy = (bx0 + bx1) / 2, (by0 + by1) / 2
        cands = sorted(((x, y) for x in _frange(bx0 + sx, bx1 - sx, step) for y in _frange(by0 + sy, by1 - sy, step)),
                       key=lambda p: (p[0] - cx) ** 2 + (p[1] - cy) ** 2)
        for x, y in cands:
            if all(x + sx < lo[0] or x - sx > hi[0] or y + sy < lo[1] or y - sy > hi[1] for lo, hi in boxes):
                return [x, y]
        return [cx, cy]

    def _create_object(self, name: str, pieces, item_tr: Affine, extruder=None, settings=None) -> str:
        """pieces: [(V, T, attrs|None, comp_transform, subtype, part_name, part_meta)] → id del objeto nuevo."""
        oid = self._new_id()
        path = self._new_object_file(name, oid)
        comps = []
        for V, T, attrs, ctr, _st, _pn, _pm in pieces:
            sub = self._new_id()
            self._append_mesh_object(path, sub, V, T, attrs)
            comps.append((sub, ctr))
        oid = self._new_id()  # el objeto principal lleva id mayor que sus piezas, como en OrcaSlicer
        comp_xml = "\n".join(f'    <component p:path="/{path}" objectid="{s}" p:UUID="{_uuid()}" transform="{c.to_3mf()}"/>'
                             for s, c in comps)
        obj_xml = (f'  <object id="{oid}" p:UUID="{_uuid()}" type="model">\n   <components>\n{comp_xml}\n'
                   f'   </components>\n  </object>\n ')
        i = self.model.rfind("</resources>")
        self.model = self.model[:i] + obj_xml + self.model[i:]
        item = f'<item objectid="{oid}" p:UUID="{_uuid()}" transform="{item_tr.to_3mf()}" printable="1"/>'
        j = self.model.rfind("</build>")
        if j < 0:  # <build/> vacío
            j = self.model.rfind("<build")
            end = self.model.find(">", j) + 1
            self.model = self.model[:j] + f'<build>\n  {item}\n </build>' + self.model[end:]
        else:
            self.model = self.model[:j] + f" {item}\n " + self.model[j:]
        node = ET.SubElement(self.settings, "object", {"id": str(oid)})
        # el objeto debe ir antes de <plate>/<assemble>, como lo escribe OrcaSlicer
        self.settings.remove(node)
        pos = next((k for k, c in enumerate(self.settings) if c.tag != "object"), len(self.settings))
        self.settings.insert(pos, node)
        ET.SubElement(node, "metadata", {"key": "name", "value": name})
        ET.SubElement(node, "metadata", {"key": "extruder", "value": str(int(extruder or 1))})
        for k, v in (settings or {}).items():
            ET.SubElement(node, "metadata", {"key": str(k), "value": serialize_value(v)})
        for (s, _c), (_V, _T, _a, _ct, st, pn, pm) in zip(comps, pieces):
            p = ET.SubElement(node, "part", {"id": str(s), "subtype": st})
            ET.SubElement(p, "metadata", {"key": "name", "value": pn})
            ET.SubElement(p, "metadata", {"key": "matrix", "value": "1 0 0 0 0 1 0 0 0 0 1 0 0 0 0 1"})
            for k, v in (pm or {}).items():
                ET.SubElement(p, "metadata", {"key": k, "value": v})
        self._add_plate_instance(oid, 0)
        self._add_assemble(oid, 0, item_tr)
        self.dirty_model = self.dirty_settings = True
        self._mesh_cache.clear()
        return str(oid)

    def add_object(self, op: dict) -> str:
        V, T = _mesh_for_op(op)
        lo = [min(p[k] for p in V) for k in range(3)]
        hi = [max(p[k] for p in V) for k in range(3)]
        c = [(lo[k] + hi[k]) / 2 for k in range(3)]
        V = [[p[k] - c[k] for k in range(3)] for p in V]  # centrado en el origen, como al importar en OrcaSlicer
        pos = op.get("position")
        xy = [float(pos[0]), float(pos[1])] if pos else self._free_spot([hi[0] - lo[0], hi[1] - lo[1]])
        R = _rotation_xyz(op.get("rotate") or {})
        tr = Affine(R, [xy[0], xy[1], 0.0])
        wz = min(tr.apply(p)[2] for p in V)
        tr.t[2] = -wz
        name = op.get("name") or (os.path.splitext(os.path.basename(op["file"]))[0] if op.get("file")
                                  else str(op.get("shape", "forma")))
        self._create_object(name, [(V, T, None, Affine(), "normal_part", name, {})], tr,
                            op.get("extruder"), op.get("settings"))
        return f"objeto {name!r} añadido en ({xy[0]:.1f}, {xy[1]:.1f}) con {len(T)} triángulos"

    def add_instance(self, op: dict) -> str:
        """Añade copias del objeto: en huecos libres de la cama, o desplazadas offset [dx, dy] cada una."""
        obj = self.resolve(op.get("object", 0))
        count = int(op.get("count", 1))
        if not 1 <= count <= 200:
            raise BridgeError("count debe estar entre 1 y 200.")
        lo, hi = self._object_world_bbox(obj)
        off = op.get("offset")
        n0 = len(self._instance_tags(obj["object_id"]))
        for k in range(1, count + 1):
            tags = self._instance_tags(obj["object_id"])
            _s, last_e, last_tag = tags[-1]
            a = _parse_attrs(last_tag)
            first = Affine.parse(_parse_attrs(tags[0][2]).get("transform"))
            tr = Affine([row[:] for row in first.A], list(first.t))
            if off:
                tr.t[0] += float(off[0]) * k
                tr.t[1] += float(off[1]) * (k if len(off) > 1 else 0)
            else:
                spot = self._free_spot([hi[0] - lo[0], hi[1] - lo[1]])
                tr.t[0] += spot[0] - (lo[0] + hi[0]) / 2
                tr.t[1] += spot[1] - (lo[1] + hi[1]) / 2
            tag = (f'<item objectid="{obj["object_id"]}" p:UUID="{_uuid()}" transform="{tr.to_3mf()}" '
                   f'printable="{a.get("printable", "1")}"/>')
            self.model = self.model[:last_e] + "\n  " + tag + self.model[last_e:]
            self._add_plate_instance(obj["object_id"], n0 + k - 1)
            self._add_assemble(obj["object_id"], n0 + k - 1, tr)
        self.dirty_model = True
        return f"{obj['name']}: {count} copia(s) añadida(s); ahora tiene {n0 + count}"

    def remove_instance(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        k = int(op.get("instance", len(obj["instances"]) - 1))
        tags = self._instance_tags(obj["object_id"])
        if len(tags) <= 1:
            raise BridgeError("Es la única instancia; usa delete para borrar el objeto.")
        if not 0 <= k < len(tags):
            raise BridgeError(f"No existe la instancia {k}.")
        s, e, _t = tags[k]
        ls = self.model.rfind("\n", 0, s)
        self.model = self.model[:ls if ls != -1 else s] + self.model[e:]
        oid = str(obj["object_id"])
        for pl in self.settings.findall("plate"):
            for mi in list(pl.findall("model_instance")):
                if self._meta(mi, "object_id") == oid:
                    ii = int(self._meta(mi, "instance_id") or 0)
                    if ii == k:
                        pl.remove(mi)
                    elif ii > k:
                        self._set_meta(mi, "instance_id", ii - 1)
        asm = self.settings.find("assemble")
        if asm is not None:
            for it in list(asm.findall("assemble_item")):
                if it.get("object_id") == oid:
                    ii = int(it.get("instance_id", 0))
                    if ii == k:
                        asm.remove(it)
                    elif ii > k:
                        it.set("instance_id", str(ii - 1))
        self.dirty_model = self.dirty_settings = True
        return f"{obj['name']}: instancia {k} eliminada"

    def split_to_parts(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        idx = int(op.get("part", 0))
        path, text, m, V, T, attrs = self._read_part_mesh(obj, idx)
        groups = _connected_components(T)
        if len(groups) < 2:
            return f"{obj['name']}: la pieza {idx} ya es de una sola pieza"
        comps = self._components(obj["object_id"])
        ctr = Affine.parse(_parse_attrs(comps[idx]).get("transform"))
        parts = self._part_nodes(obj)
        pnode = parts[idx] if idx < len(parts) else None
        subtype = pnode.get("subtype", "normal_part") if pnode is not None else "normal_part"
        pname = (self._meta(pnode, "name") if pnode is not None else None) or obj["name"]
        pmeta = {mm.get("key"): mm.get("value") for mm in (pnode.findall("metadata") if pnode is not None else [])
                 if mm.get("key") not in ("name", "matrix", "source_object_id", "source_volume_id")}
        groups.sort(key=len, reverse=True)
        pieces = [_submesh(V, T, attrs, g) for g in groups]
        self._write_part_mesh(path, text, m, pieces[0][0], pieces[0][1], pieces[0][2])
        node = self._require_node(obj)
        for n, (V2, T2, A2) in enumerate(pieces[1:], start=2):
            sub = self._new_id()
            self._append_mesh_object(path, sub, V2, T2, A2)
            self._add_component(obj["object_id"], path, sub, ctr)
            p = ET.SubElement(node, "part", {"id": str(sub), "subtype": subtype})
            ET.SubElement(p, "metadata", {"key": "name", "value": f"{pname} ({n})"})
            ET.SubElement(p, "metadata", {"key": "matrix", "value": "1 0 0 0 0 1 0 0 0 0 1 0 0 0 0 1"})
            for k, v in pmeta.items():
                ET.SubElement(p, "metadata", {"key": k, "value": v})
        self.dirty_settings = True
        return f"{obj['name']}: pieza {idx} dividida en {len(groups)} piezas"

    def split_to_objects(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        if len(obj["instances"]) > 1:
            raise BridgeError("Divide en objetos solo objetos con una instancia (quita las copias antes).")
        parts = self._part_nodes(obj)
        solid = [i for i, p in enumerate(parts) if p.get("subtype", "normal_part") == "normal_part"]
        if len(solid) == 1:
            self.split_to_parts({"object": obj["index"], "part": solid[0]})
            obj = self.resolve(obj["index"])
            parts = self._part_nodes(obj)
            solid = [i for i, p in enumerate(parts) if p.get("subtype", "normal_part") == "normal_part"]
        if len(solid) < 2:
            return f"{obj['name']}: no hay nada que dividir"
        item_tr = Affine.parse(obj["instances"][0]["transform"])
        node = self._require_node(obj)
        obj_meta = {mm.get("key"): mm.get("value") for mm in node.findall("metadata") if mm.get("key") != "name"}
        moved = 0
        for i in reversed(solid[1:]):
            comps = self._components(obj["object_id"])
            a = _parse_attrs(comps[i])
            path = a.get("p:path", "").lstrip("/") or MAIN_MODEL
            ctr = Affine.parse(a.get("transform"))
            V = [list(p) for p in self._vertices(path, a["objectid"])[0][1]]
            text = self.data[path].decode("utf-8")
            mm = re.search(r'(<object\b[^>]*\bid="%s"[^>]*>\s*<mesh>)(.*?)(</mesh>)' % re.escape(a["objectid"]), text, re.S)
            T, A = [], []
            for tag in re.findall(r"<triangle\b[^>]*/>", mm.group(2)):
                ta = _parse_attrs(tag)
                T.append((int(ta.pop("v1")), int(ta.pop("v2")), int(ta.pop("v3"))))
                A.append(ta)
            pnode = parts[i]
            pname = self._meta(pnode, "name") or f"{obj['name']} {i}"
            pmeta = {m2.get("key"): m2.get("value") for m2 in pnode.findall("metadata")
                     if m2.get("key") not in ("name", "matrix", "source_object_id", "source_volume_id")}
            self._create_object(pname, [(V, T, A, ctr, "normal_part", pname, pmeta)], item_tr,
                                obj_meta.get("extruder"), {k: v for k, v in obj_meta.items() if k != "extruder"})
            self._remove_component(obj["object_id"], i)
            self._remove_mesh_object(path, a["objectid"])
            node.remove(pnode)
            moved += 1
        self.dirty_settings = True
        return f"{obj['name']}: dividido en {moved + 1} objetos"

    def cut(self, op: dict) -> str:
        """Corta un objeto por un plano y tapa las caras cortadas (como la herramienta Cortar de OrcaSlicer).
        axis x|y|z + at (mm, coordenada de la cama) o plane {point, normal}. keep both|upper|lower.
        place_on_cut (true): la parte de arriba se gira para apoyarse en la cara del corte."""
        obj = self.resolve(op.get("object", 0))
        if len(obj["instances"]) > 1:
            raise BridgeError("Corta objetos con una sola instancia (quita las copias antes).")
        if op.get("plane"):
            pt = [float(v) for v in op["plane"]["point"]]
            n = [float(v) for v in op["plane"]["normal"]]
        else:
            axis = str(op.get("axis", "z")).lower()
            if axis not in "xyz" or len(axis) != 1:
                raise BridgeError("axis debe ser x, y o z.")
            k = "xyz".index(axis)
            n = [0.0, 0.0, 0.0]
            n[k] = 1.0
            pt = [0.0, 0.0, 0.0]
            pt[k] = float(op["at"])
        ln = math.sqrt(sum(v * v for v in n)) or 1.0
        n = [v / ln for v in n]
        d = sum(n[k] * pt[k] for k in range(3))
        keep = str(op.get("keep", "both"))
        if keep not in ("both", "upper", "lower"):
            raise BridgeError("keep debe ser both, upper o lower.")
        node = self._require_node(obj)
        obj_meta = {m.get("key"): m.get("value") for m in node.findall("metadata") if m.get("key") != "name"}
        pnodes = node.findall("part")
        halves = {True: [], False: []}
        for pi in range(len(self._components(obj["object_id"]))):
            W = self._world_transform(obj, pi)
            _p, _t, _m, V, T, A = self._read_part_mesh(obj, pi)
            Wv = [W.apply(p) for p in V]
            if _signed_volume(Wv, T) < 0:        # transformación con reflejo: reorienta
                T = [(a, c, b) for a, b, c in T]
            up, down = _plane_split(Wv, T, A, n, d)
            pn = pnodes[pi] if pi < len(pnodes) else None
            sub = pn.get("subtype", "normal_part") if pn is not None else "normal_part"
            meta = {m.get("key"): m.get("value") for m in (pn.findall("metadata") if pn is not None else [])
                    if m.get("key") not in ("name", "matrix", "source_object_id", "source_volume_id")}
            pname = (self._meta(pn, "name") if pn is not None else None) or obj["name"]
            for side, half in ((True, up), (False, down)):
                if half[1]:
                    capped = _close_cut(half, [-v for v in n] if side else n)
                    halves[side].append((capped, sub, pname, meta))
        if not any(sub == "normal_part" for _h, sub, _n, _m in halves[True]) or \
                not any(sub == "normal_part" for _h, sub, _n, _m in halves[False]):
            raise BridgeError("El plano no corta el objeto (todo queda a un lado).")
        made = []
        for side, label in ((False, "abajo"), (True, "arriba")):
            if keep == ("lower" if side else "upper"):
                continue
            pieces = halves[side]
            allv = [p for (V, _T, _A), sub, _n, _m in pieces if sub == "normal_part" for p in V]
            c = [(min(p[k] for p in allv) + max(p[k] for p in allv)) / 2 for k in range(3)]
            R = [[1.0, 0, 0], [0, 1.0, 0], [0, 0, 1.0]]
            if side and op.get("place_on_cut", True):
                R = _rotation_to(n, [0.0, 0.0, 1.0])      # la cara del corte (normal -n) queda mirando a la cama
            pl = []
            for (V, T, A), sub, pname, meta in pieces:
                Vl = [[sum(R[r][k] * (p[k] - c[k]) for k in range(3)) for r in range(3)] for p in V]
                pl.append((Vl, T, A, Affine(), sub, f"{pname} ({label})", meta))
            name = f"{obj['name']} ({label})"
            tr = Affine(None, [c[0], c[1], 0.0])
            zmin = min(p[2] for (V, _t, _a, _c, sub, _n, _m) in pl if sub == "normal_part" for p in V)
            tr.t[2] = -zmin
            if side and keep == "both":
                size = [max(p[k] for p in pl[0][0]) - min(p[k] for p in pl[0][0]) for k in range(2)]
                spot = self._free_spot(size)
                tr.t[0], tr.t[1] = spot
            made.append(self._create_object(name, pl, tr, obj_meta.get("extruder"),
                                            {k: v for k, v in obj_meta.items() if k != "extruder"}))
        self._delete_object_force(obj["object_id"])
        return f"{obj['name']}: cortado por el plano n={[round(v, 3) for v in n]} d={round(d, 3)} → {len(made)} objeto(s)"

    def _delete_object_force(self, oid: str):
        """Como _delete_object pero sin exigir que quede otro objeto (el corte ya ha creado los nuevos)."""
        objs = self.objects()
        if len(objs) <= 1:
            raise BridgeError("No se puede eliminar el último objeto del proyecto.")
        self._delete_object(oid)

    # ── capas: rangos de altura, altura de capa variable, G-code por capa ──
    def _xml_file(self, name: str, root_tag: str):
        if name in self.data:
            try:
                return ET.fromstring(self.data[name])
            except ET.ParseError:
                pass
        return ET.Element(root_tag)

    def _put_xml(self, name: str, root):
        ET.indent(root, space=" ")
        self.data[name] = b'<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(root, encoding="utf-8")
        if name not in self.names:
            self.names.append(name)

    def set_layer_range(self, op: dict) -> str:
        """Modificador de rango de altura: ajustes distintos entre min_z y max_z (como 'Añadir rango de altura')."""
        obj = self.resolve(op.get("object", 0))
        z0, z1 = float(op["min_z"]), float(op["max_z"])
        if z1 <= z0:
            raise BridgeError("max_z debe ser mayor que min_z.")
        settings = dict(op.get("settings") or {})
        if "layer_height" not in settings:
            settings["layer_height"] = (self.project or {}).get("layer_height", "0.2")
        root = self._xml_file(LAYER_RANGES_FILE, "objects")
        oidx = str(self._model_index(obj["object_id"]) + 1)
        onode = next((o for o in root.findall("object") if o.get("id") == oidx), None)
        if onode is None:
            onode = ET.SubElement(root, "object", {"id": oidx})
        for r in list(onode.findall("range")):
            a0, a1 = float(r.get("min_z")), float(r.get("max_z"))
            if a0 < z1 and z0 < a1:
                if op.get("replace", True):
                    onode.remove(r)
                else:
                    raise BridgeError(f"Se solapa con el rango {a0}–{a1}.")
        rng = ET.Element("range", {"min_z": _fmt(z0), "max_z": _fmt(z1)})
        for k, v in settings.items():
            ET.SubElement(rng, "option", {"opt_key": str(k)}).text = serialize_value(v)
        ranges = onode.findall("range") + [rng]
        for r in onode.findall("range"):
            onode.remove(r)
        for r in sorted(ranges, key=lambda r: float(r.get("min_z"))):
            onode.append(r)
        self._put_xml(LAYER_RANGES_FILE, root)
        return f"{obj['name']}: rango {z0}–{z1} mm con {sorted(settings)}"

    def clear_layer_ranges(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        root = self._xml_file(LAYER_RANGES_FILE, "objects")
        oidx = str(self._model_index(obj["object_id"]) + 1)
        for o in list(root.findall("object")):
            if o.get("id") == oidx:
                root.remove(o)
        if len(root):
            self._put_xml(LAYER_RANGES_FILE, root)
        elif LAYER_RANGES_FILE in self.data:
            del self.data[LAYER_RANGES_FILE]
            self.names.remove(LAYER_RANGES_FILE)
        return f"{obj['name']}: rangos de altura eliminados"

    def _read_profiles(self):
        out = {}
        raw = self.data.get(LAYER_PROFILE_FILE, b"").decode("utf-8")
        for line in raw.splitlines():
            if line.startswith("object_id=") and "|" in line:
                k, v = line.split("|", 1)
                out[k.split("=")[1]] = v
        return out

    def _write_profiles(self, prof: dict):
        if prof:
            self.data[LAYER_PROFILE_FILE] = "".join(f"object_id={k}|{v}\n" for k, v in
                                                    sorted(prof.items(), key=lambda kv: int(kv[0]))).encode()
            if LAYER_PROFILE_FILE not in self.names:
                self.names.append(LAYER_PROFILE_FILE)
        elif LAYER_PROFILE_FILE in self.data:
            del self.data[LAYER_PROFILE_FILE]
            self.names.remove(LAYER_PROFILE_FILE)

    def set_variable_layer_height(self, op: dict) -> str:
        """Altura de capa variable del objeto (perfil de OrcaSlicer: puntos z;altura interpolados).
        ranges [{min_z, max_z, layer_height}] → escalones con la altura base fuera de ellos;
        points [[z, altura], ...] → perfil libre (se extiende de 0 a la altura del objeto)."""
        obj = self.resolve(op.get("object", 0))
        lo, hi = self._object_world_bbox(obj)
        height = round(hi[2] - lo[2], 4)
        lh = (self.project or {}).get("layer_height", 0.2)
        base = float(op.get("base", lh[0] if isinstance(lh, list) else lh))
        eps = 1e-3
        if op.get("ranges"):
            rs = sorted(((float(r["min_z"]), float(r["max_z"]), float(r["layer_height"])) for r in op["ranges"]))
            pts = []
            z = 0.0
            for z0, z1, h in rs:
                if z1 <= z0:
                    raise BridgeError("Cada rango necesita max_z > min_z.")
                if z0 > z + eps:
                    pts += [(z, base), (z0 - eps, base)]
                pts += [(max(z0, 0.0), h), (min(z1, height), h)]
                z = min(z1, height) + eps
            if z < height:
                pts += [(z, base), (height, base)]
        elif op.get("points"):
            pts = sorted((float(z), float(h)) for z, h in op["points"])
            if pts[0][0] > 0:
                pts.insert(0, (0.0, pts[0][1]))
            if pts[-1][0] < height:
                pts.append((height, pts[-1][1]))
        else:
            raise BridgeError("Indica ranges [{min_z, max_z, layer_height}] o points [[z, altura], ...].")
        for _z, h in pts:
            if not 0.01 <= h <= 2.0:
                raise BridgeError(f"Altura de capa fuera de rango: {h}")
        proj = self.project or {}

        def _lim(k, d):
            v = proj.get(k, d)
            return float(v[0] if isinstance(v, list) else v)
        hmin, hmax = _lim("min_layer_height", 0.07), _lim("max_layer_height", 10)
        clamped = [h for _z, h in pts if not hmin <= h <= hmax]
        pts = [(z, min(max(h, hmin), hmax)) for z, h in pts]
        # OrcaSlicer descarta el perfil si la primera altura no es la de la primera capa
        # (PrintObject::update_layer_height_profile) o si el último punto no está en la cima del objeto.
        ih = (self.project or {}).get("initial_layer_print_height", base)
        h_first = float(ih[0] if isinstance(ih, list) else ih)
        pts = [(z, h) for z, h in pts if z >= h_first - 1e-9]
        if not pts or pts[0][0] > h_first + 1e-9:
            pts.insert(0, (h_first, pts[0][1] if pts else base))
        pts.insert(0, (0.0, h_first))
        pts[-1] = (height, pts[-1][1])
        prof = self._read_profiles()
        prof[str(self._model_index(obj["object_id"]) + 1)] = ";".join(f"{z:f};{h:f}" for z, h in pts)
        self._write_profiles(prof)
        warn = f"; ajustadas a {hmin}–{hmax} mm (límites de la boquilla): {clamped}" if clamped else ""
        return f"{obj['name']}: altura de capa variable con {len(pts)} puntos (altura del objeto {height} mm){warn}"

    def clear_variable_layer_height(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        prof = self._read_profiles()
        prof.pop(str(self._model_index(obj["object_id"]) + 1), None)
        self._write_profiles(prof)
        return f"{obj['name']}: altura de capa variable eliminada"

    def _layer_z(self, layer: int) -> float:
        P = self.project or {}

        def num(k, d):
            v = P.get(k, d)
            return float(v[0] if isinstance(v, list) else v)
        h0, h = num("initial_layer_print_height", 0.2), num("layer_height", 0.2)
        return round(h0 + (int(layer) - 1) * h, 4)

    def add_layer_gcode(self, op: dict) -> str:
        """Cambio de color, pausa, G-code propio o cambio de herramienta a una altura (o número de capa)."""
        kinds = {"color_change": 0, "pause": 1, "tool_change": 2, "template": 3, "custom": 4}
        kind = str(op.get("type", "pause"))
        if kind not in kinds:
            raise BridgeError(f"type debe ser uno de {sorted(kinds)}")
        note = ""
        if kind == "color_change":
            # OrcaSlicer 2.5 no emite nada para ColorChange (código desactivado en GCode.cpp): el cambio de color
            # se hace como cambio de filamento, que ejecuta el G-code de cambio de filamento de la impresora (M600…).
            cols = (self.project or {}).get("filament_colour") or []
            ext = op.get("extruder")
            if op.get("color") or ext is None or int(ext) > len(cols):
                if not op.get("color"):
                    raise BridgeError("color_change necesita color (#RRGGBB) o extruder de un filamento existente.")
                match = [i + 1 for i, c in enumerate(cols) if str(c).upper() == str(op["color"]).upper()]
                if match:
                    ext = match[0]
                else:
                    self.add_filament({"copy_from": 1, "color": op["color"]})
                    ext = len(self.project["filament_colour"])
                    note = f" (filamento {ext} añadido con color {op['color']})"
            op = dict(op, type="tool_change", extruder=int(ext))
            kind = "tool_change"
        z = float(op["z"]) if op.get("z") is not None else self._layer_z(op["layer"]) if op.get("layer") else None
        if z is None:
            raise BridgeError("Indica z (mm, altura superior de la capa) o layer (número de capa, 1 = primera).")
        plate = int(op.get("plate", 1))
        root = self._xml_file(CUSTOM_GCODE_FILE, "custom_gcodes_per_layer")
        if root.tag != "custom_gcodes_per_layer":
            root = ET.Element("custom_gcodes_per_layer")
        pnode = None
        for pl in root.findall("plate"):
            info = pl.find("plate_info")
            if info is not None and info.get("id") == str(plate):
                pnode = pl
        if pnode is None:
            pnode = ET.SubElement(root, "plate")
            ET.SubElement(pnode, "plate_info", {"id": str(plate)})
        n_fil = len((self.project or {}).get("filament_colour") or [1])
        extruder = int(op.get("extruder", 1))
        P = self.project or {}
        gcode = {"pause": str(P.get("machine_pause_gcode", "M601")), "tool_change": "tool_change",
                 "template": str(P.get("template_custom_gcode", ""))}.get(kind, str(op.get("gcode", "")))
        extra = str(op.get("message", "")) if kind == "pause" else str(op.get("gcode", "")) if kind == "custom" else ""
        color = str(op.get("color", ""))
        if kind == "color_change" and not color:
            cols = P.get("filament_colour") or ["#FFFFFF"]
            color = cols[min(extruder, len(cols)) - 1]
        for old in list(pnode.findall("layer")):
            if abs(float(old.get("top_z", -1)) - z) < 1e-4:
                pnode.remove(old)
        el = ET.Element("layer", {"top_z": _fmt(z), "type": str(kinds[kind]), "extruder": str(extruder),
                                  "color": color, "extra": extra, "gcode": gcode})
        layers = pnode.findall("layer") + [el]
        mode = pnode.find("mode")
        for c in list(pnode):
            if c.tag in ("layer", "mode"):
                pnode.remove(c)
        for c in sorted(layers, key=lambda c: float(c.get("top_z"))):
            pnode.append(c)
        mode = ET.Element("mode", {"value": "SingleExtruder" if n_fil <= 1 else "MultiAsSingle"})
        pnode.append(mode)
        self._put_xml(CUSTOM_GCODE_FILE, root)
        return (f"placa {plate}: {kind}{' al filamento ' + str(extruder) if kind == 'tool_change' else ''} a Z={z} mm"
                + (f" (capa {op['layer']})" if op.get("layer") else "") + note)

    def clear_layer_gcode(self, op: dict) -> str:
        if CUSTOM_GCODE_FILE not in self.data:
            return "no había G-code por capa"
        root = self._xml_file(CUSTOM_GCODE_FILE, "custom_gcodes_per_layer")
        plate = op.get("plate")
        n = 0
        for pl in list(root.findall("plate")):
            info = pl.find("plate_info")
            if plate is None or (info is not None and info.get("id") == str(plate)):
                for c in list(pl.findall("layer")):
                    if op.get("z") is None or abs(float(c.get("top_z")) - float(op["z"])) < 1e-3:
                        pl.remove(c)
                        n += 1
        self._put_xml(CUSTOM_GCODE_FILE, root)
        return f"{n} G-code por capa eliminados"

    # ── orejas de borde ─────────────────────────────────────────────────
    def set_brim_ears(self, op: dict) -> str:
        obj = self.resolve(op.get("object", 0))
        radius = float(op.get("radius", 4.0))
        inst = Affine.parse(obj["instances"][0]["transform"])
        inv = inst.inverse()
        lo, hi = self._object_world_bbox(obj)
        if op.get("points"):
            world = [[float(p[0]), float(p[1]), lo[2]] for p in op["points"]]
        else:
            world = self._auto_ear_points(obj, float(op.get("max_angle", 125)))
        local = [inv.apply(p) for p in world]
        lines = []
        raw = self.data.get(BRIM_EARS_FILE, b"").decode("utf-8").splitlines()
        oidx = str(self._model_index(obj["object_id"]) + 1)
        lines = [ln for ln in raw[1:] if ln.strip() and not ln.startswith(f"object_id={oidx}|")]
        if local:
            lines.append(f"object_id={oidx}|" + " ".join(f"{p[0]:f} {p[1]:f} {p[2]:f} {radius:f}" for p in local))
        if lines:
            self.data[BRIM_EARS_FILE] = ("brim_points_format_version=0\n" + "\n".join(lines) + "\n").encode()
            if BRIM_EARS_FILE not in self.names:
                self.names.append(BRIM_EARS_FILE)
        node = self._require_node(obj)
        self._set_meta(node, "brim_type", "brim_ears" if local else "auto_brim")
        self.dirty_settings = True
        return f"{obj['name']}: {len(local)} orejas de borde (radio {radius} mm)"

    def _auto_ear_points(self, obj, max_angle):
        """Esquinas de la huella del objeto en la cama (envolvente convexa de la primera capa)."""
        lo, _hi = self._object_world_bbox(obj)
        pts = []
        inst = Affine.parse(obj["instances"][0]["transform"])
        for ctr, verts in self._vertices(MAIN_MODEL, obj["object_id"]):
            T = inst @ ctr
            for v in verts:
                p = T.apply(v)
                if p[2] <= lo[2] + 0.3:
                    pts.append((p[0], p[1]))
        hull = _convex_hull(pts)
        out = []
        n = len(hull)
        for i in range(n):
            a, b, c = hull[i - 1], hull[i], hull[(i + 1) % n]
            u = (a[0] - b[0], a[1] - b[1])
            v = (c[0] - b[0], c[1] - b[1])
            lu, lv = math.hypot(*u), math.hypot(*v)
            if lu < 1e-6 or lv < 1e-6:
                continue
            ang = math.degrees(math.acos(max(-1.0, min(1.0, (u[0] * v[0] + u[1] * v[1]) / (lu * lv)))))
            if ang <= max_angle:
                out.append([b[0], b[1], lo[2]])
        return out

    # ── placas e información del proyecto ──────────────────────────────
    def set_plate(self, op: dict) -> str:
        plates = self.settings.findall("plate")
        k = int(op.get("plate", 1))
        if not 1 <= k <= len(plates):
            raise BridgeError(f"No existe la placa {k} (hay {len(plates)}).")
        pl = plates[k - 1]
        keys = {"name": "plater_name", "bed_type": "bed_type", "print_sequence": "print_sequence",
                "spiral_mode": "spiral_mode", "locked": "locked"}
        done = []
        for k_in, k_out in keys.items():
            if k_in in op:
                v = op[k_in]
                v = ("true" if v else "false") if isinstance(v, bool) and k_in == "locked" else \
                    ("1" if v else "0") if isinstance(v, bool) else str(v)
                self._set_meta(pl, k_out, v)
                done.append(k_in)
        self.dirty_settings = True
        return f"placa {k}: {', '.join(done) or 'sin cambios'}"

    def set_project_info(self, op: dict) -> str:
        done = []
        for k, tag in PROJECT_INFO_TAGS.items():
            if k not in op:
                continue
            val = _xml_text(str(op[k]))
            pat = re.compile(r'(<metadata name="%s">)(.*?)(</metadata>)' % tag, re.S)
            if pat.search(self.model):
                self.model = pat.sub(lambda m: m.group(1) + val + m.group(3), self.model, count=1)
            else:
                i = self.model.find("<resources")
                self.model = self.model[:i] + f'<metadata name="{tag}">{val}</metadata>\n ' + self.model[i:]
            done.append(k)
        self.dirty_model = True
        return f"información del proyecto: {', '.join(done)}"

    # ── análisis ────────────────────────────────────────────────────────
    def analyze(self, overhang_angle: float = 45.0) -> list[dict]:
        out = []
        limit = -math.sin(math.radians(overhang_angle))
        for obj in self.objects():
            parts_out = []
            olo, ohi = self._object_world_bbox(obj)
            node = self._settings_object(obj["object_id"])
            pnodes = node.findall("part") if node is not None else []
            for pi in range(len(self._components(obj["object_id"]))):
                W = self._world_transform(obj, pi)
                try:
                    _p, _t, _m, V, T, attrs = self._read_part_mesh(obj, pi)
                except BridgeError:
                    continue
                Wv = [W.apply(p) for p in V]
                vol = area = over = bottom = 0.0
                for (i, j, k) in T:
                    p0, p1, p2 = Wv[i], Wv[j], Wv[k]
                    vol += (p0[0] * (p1[1] * p2[2] - p1[2] * p2[1]) - p0[1] * (p1[0] * p2[2] - p1[2] * p2[0])
                            + p0[2] * (p1[0] * p2[1] - p1[1] * p2[0])) / 6.0
                    u = [p1[q] - p0[q] for q in range(3)]
                    v = [p2[q] - p0[q] for q in range(3)]
                    cr = [u[1] * v[2] - u[2] * v[1], u[2] * v[0] - u[0] * v[2], u[0] * v[1] - u[1] * v[0]]
                    ar = 0.5 * math.sqrt(sum(c * c for c in cr))
                    area += ar
                    if ar > 0:
                        nz = cr[2] / (2 * ar)
                        if max(p0[2], p1[2], p2[2]) <= olo[2] + 0.05 and nz < -0.99:
                            bottom += ar
                        elif nz < limit:
                            over += ar
                paint = {}
                for layer, attr in PAINT_LAYERS.items():
                    c = collections.Counter(at[attr] for at in attrs if at.get(attr))
                    if c:
                        paint[layer] = {_describe_code(layer, k): n for k, n in c.items()}
                pn = pnodes[pi] if pi < len(pnodes) else None
                parts_out.append({
                    "part": pi, "name": (self._meta(pn, "name") if pn is not None else "") or "",
                    "type": _PART_TYPE_NAMES.get(pn.get("subtype", "normal_part") if pn is not None else "normal_part"),
                    "triangles": len(T), "volume_mm3": round(abs(vol), 2), "area_mm2": round(area, 2),
                    "overhang_area_mm2": round(over, 2), "bed_contact_mm2": round(bottom, 2), "painted": paint})
            solid = [p for p in parts_out if p["type"] == "part"]
            out.append({"index": obj["index"], "name": obj["name"], "instances": len(obj["instances"]),
                        "bbox_min": [round(v, 3) for v in olo], "bbox_max": [round(v, 3) for v in ohi],
                        "size": [round(ohi[k] - olo[k], 3) for k in range(3)],
                        "volume_mm3": round(sum(p["volume_mm3"] for p in solid), 2),
                        "overhang_area_mm2": round(sum(p["overhang_area_mm2"] for p in solid), 2),
                        "parts": parts_out})
        return out

    def layer_info(self) -> dict:
        ranges = {}
        root = self._xml_file(LAYER_RANGES_FILE, "objects")
        for o in root.findall("object"):
            ranges[o.get("id")] = [{"min_z": float(r.get("min_z")), "max_z": float(r.get("max_z")),
                                    "settings": {op.get("opt_key"): op.text for op in r.findall("option")}}
                                   for r in o.findall("range")]
        gcodes = []
        if CUSTOM_GCODE_FILE in self.data:
            for pl in self._xml_file(CUSTOM_GCODE_FILE, "x").findall("plate"):
                info = pl.find("plate_info")
                for c in pl.findall("layer"):
                    gcodes.append({"plate": int(info.get("id")) if info is not None else 1, **c.attrib})
        return {"layer_ranges_by_object": ranges, "variable_layer_height_by_object": self._read_profiles(),
                "layer_gcodes": gcodes,
                "brim_ears": self.data.get(BRIM_EARS_FILE, b"").decode("utf-8") or None,
                "plates": [{m.get("key"): m.get("value") for m in pl.findall("metadata")}
                           for pl in self.settings.findall("plate")]}

    def add_filament(self, op: dict) -> str:
        """Añade una ranura de filamento al proyecto copiando otra, como el botón + de OrcaSlicer."""
        P = self.project
        if P is None or "filament_colour" not in P:
            raise BridgeError("El proyecto no tiene ajustes de filamento.")
        n = len(P["filament_colour"])
        src = int(op.get("copy_from", n)) - 1
        if not 0 <= src < n:
            raise BridgeError(f"copy_from debe estar entre 1 y {n}.")
        for k in list(P):
            v = P[k]
            if k in FILAMENT_VECTOR_KEYS and isinstance(v, list) and len(v) == n:
                v.append(v[src])
        if isinstance(P.get("filament_self_index"), list):
            P["filament_self_index"].append(str(n + 1))
        for k in ("different_settings_to_system", "inherits_group"):  # [proceso, filamentos..., impresora]
            v = P.get(k)
            if isinstance(v, list) and len(v) == n + 2:
                v.insert(n + 1, v[1 + src])
        v = P.get("compatible_machine_expression_group")  # [proceso, filamentos...]
        if isinstance(v, list) and len(v) == n + 1:
            v.append(v[1 + src])
        v = P.get("flush_volumes_vector")
        if isinstance(v, list) and len(v) == 2 * n:
            v += v[2 * src:2 * src + 2]
        v = P.get("flush_volumes_matrix")
        if isinstance(v, list) and len(v) == n * n:
            flush = str(op.get("flush_volume", 280))
            M = [v[r * n:(r + 1) * n] for r in range(n)]
            for r in range(n):
                M[r].append(flush)
            M.append([flush] * n + ["0"])
            P["flush_volumes_matrix"] = [x for row in M for x in row]
        if op.get("color"):
            P["filament_colour"][n] = str(op["color"])
        self.dirty_project = True
        return f"filamento {n + 1} añadido (copia del {src + 1}, color {P['filament_colour'][n]})"

    def set_flush_volumes(self, op: dict) -> str:
        """Fija el volumen de purga (mm³) entre todos los pares de filamentos distintos, o uno: from/to."""
        P = self.project or {}
        v = P.get("flush_volumes_matrix")
        n = len(P.get("filament_colour") or [])
        if not isinstance(v, list) or len(v) != n * n:
            raise BridgeError("El proyecto no tiene matriz de purga.")
        val = str(int(float(op.get("value", 280))))
        pairs = [(int(op["from"]) - 1, int(op["to"]) - 1)] if "from" in op else \
            [(r, c) for r in range(n) for c in range(n) if r != c]
        for r, c in pairs:
            v[r * n + c] = val
        self.dirty_project = True
        return f"purga {val} mm³ en {len(pairs)} cambio(s) de filamento"

    def set_filament_color(self, op: dict) -> str:
        P = self.project or {}
        cols = P.get("filament_colour")
        i = int(op.get("filament", 1)) - 1
        if not isinstance(cols, list) or not 0 <= i < len(cols):
            raise BridgeError("Filamento inexistente.")
        cols[i] = str(op["color"])
        self.dirty_project = True
        return f"filamento {i + 1}: color {cols[i]}"

    def _require_node(self, obj):
        node = self._settings_object(obj["object_id"])
        if node is None:
            node = ET.SubElement(self.settings, "object", {"id": str(obj["object_id"])})
        return node

    def _delete_object(self, oid: str):
        if len(self.objects()) <= 1:
            raise BridgeError("No se puede eliminar el último objeto del proyecto.")
        for s, e, _tag in reversed(self._instance_tags(oid)):
            # elimina también el salto de línea y la sangría previos
            ls = self.model.rfind("\n", 0, s)
            self.model = self.model[:ls if ls != -1 else s] + self.model[e:]
        m = re.search(r'<object\b[^>]*\bid="%s"[^>]*>.*?</object>' % re.escape(oid), self.model, re.S)
        if m:
            ls = self.model.rfind("\n", 0, m.start())
            self.model = self.model[:ls if ls != -1 else m.start()] + self.model[m.end():]
        node = self._settings_object(oid)
        if node is not None:
            self.settings.remove(node)
        for plate in self.settings.findall("plate"):
            for mi in list(plate.findall("model_instance")):
                if self._meta(mi, "object_id") == str(oid):
                    plate.remove(mi)
        asm = self.settings.find("assemble")
        if asm is not None:
            for it in list(asm.findall("assemble_item")):
                if it.get("object_id") == str(oid):
                    asm.remove(it)
        self.dirty_model = self.dirty_settings = True

    # ── guardado ────────────────────────────────────────────────────────
    def save(self, out_path: str):
        if self.dirty_model:
            self.data[MAIN_MODEL] = self.model.encode("utf-8")
        if self.dirty_settings:
            ET.indent(self.settings, space="  ")
            self.data[MODEL_SETTINGS] = (b'<?xml version="1.0" encoding="UTF-8"?>\n'
                                         + ET.tostring(self.settings, encoding="utf-8"))
            if MODEL_SETTINGS not in self.names:
                self.names.append(MODEL_SETTINGS)
        if self.dirty_project and self.project is not None:
            self.data[PROJECT_SETTINGS] = json.dumps(self.project, indent=4, ensure_ascii=False).encode("utf-8")
        tmp = out_path + ".tmp"
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
            for n in self.names:
                z.writestr(n, self.data[n])
        os.replace(tmp, out_path)


# ─────────────────────────────────────────────────────────────────────────────
# Lecturas (hilo de la interfaz)
# ─────────────────────────────────────────────────────────────────────────────

def _host_ui_info():
    plater = orca.host.plater()
    info = {"bridge_version": BRIDGE_VERSION,
            "project_path": _get(plater, "project_path"),
            "project_dirty": _get(plater, "is_project_dirty"),
            "presets_dirty": _get(plater, "is_presets_dirty"),
            "objects": len(list(orca.host.model().objects())),
            "current_plate": _get(orca.host.model(), "current_plate_index")}
    for fn in ("app_info", "selected_printer", "gl_info"):
        try:
            info[fn] = _jsonable(getattr(orca.host, fn)())
        except Exception as exc:  # noqa: BLE001
            info[fn] = f"no disponible: {exc}"
    return info


@bridge_method("app.info", doc="Versión de OrcaSlicer, proyecto abierto, impresora seleccionada y estado.")
def _app_info(_params):
    return _host_ui_info()


def _config_dict(obj):
    try:
        return {k: _jsonable(obj.config_value(k)) for k in obj.config_keys()}
    except Exception:  # noqa: BLE001
        return {}


@bridge_method("scene.get", doc="Objetos: piezas/modificadores, instancias, cajas, ajustes por objeto y estadísticas de malla.")
def _scene_get(params):
    mesh_stats = bool(params.get("mesh_stats", True))
    model = orca.host.model()
    out = []
    for oi, obj in enumerate(model.objects()):
        o = {"index": oi, "id": _get(obj, "id"), "name": _get(obj, "name"),
             "input_file": _get(obj, "input_file"), "printable": _get(obj, "printable"),
             "bbox": _jsonable(_get(obj, "bounding_box")), "min_z": _get(obj, "min_z"),
             "max_z": _get(obj, "max_z"), "settings": _config_dict(obj),
             "flags": {k: _get(obj, k) for k in ("is_cut", "has_custom_layering", "is_fdm_support_painted",
                                                 "is_seam_painted", "is_mm_painted", "is_fuzzy_skin_painted")},
             "volumes": [], "instances": []}
        for vi, vol in enumerate(obj.volumes()):
            v = {"index": vi, "name": _get(vol, "name"), "type": _jsonable(_get(vol, "type")),
                 "extruder": _get(vol, "extruder_id"), "settings": _config_dict(vol),
                 "offset": _jsonable(_get(vol, "offset")), "rotation": _jsonable(_get(vol, "rotation")),
                 "scale": _jsonable(_get(vol, "scaling_factor"))}
            if mesh_stats:
                v.update({"triangles": _get(vol, "facets_count"), "volume_mm3": _get(vol, "volume"),
                          "manifold": _get(vol, "is_manifold"), "repaired_errors": _get(vol, "mesh_errors_count"),
                          "bbox": _jsonable(_get(vol, "bounding_box"))})
            o["volumes"].append(v)
        for ii, inst in enumerate(obj.instances()):
            o["instances"].append({"index": ii, "offset": _jsonable(_get(inst, "offset")),
                                   "rotation_rad": _jsonable(_get(inst, "rotation")),
                                   "scale": _jsonable(_get(inst, "scaling_factor")),
                                   "mirror": _jsonable(_get(inst, "mirror")),
                                   "printable": _get(inst, "printable"),
                                   "bbox": _jsonable(_get(inst, "bounding_box"))})
        out.append(o)
    return {"objects": out, "current_plate": _get(model, "current_plate_index")}


def _preset_summary(p):
    if p is None:
        return None
    return {"name": _get(p, "name"), "label": _get(p, "label"), "alias": _get(p, "alias"),
            "dirty": _get(p, "is_dirty"), "system": _get(p, "is_system"), "user": _get(p, "is_user"),
            "bundle_id": _get(p, "bundle_id"), "inherits": _safe_cfg(p, "inherits")}


def _safe_cfg(p, key):
    try:
        return p.config_value(key)
    except Exception:  # noqa: BLE001
        return None


def _category(name: str) -> str:
    c = (name or "").lower()
    c = {"print": "process", "machine": "printer"}.get(c, c)
    if c not in COLLECTION_ATTR:
        raise BridgeError(f"Categoría no válida: {name!r}. Usa process, printer o filament.")
    return c


def _collection(cat: str):
    return getattr(orca.host.preset_bundle(), COLLECTION_ATTR[_category(cat)])


@bridge_method("presets.current", doc="Presets activos de impresora, proceso y filamentos (por ranura).")
def _presets_current(_params):
    b = orca.host.preset_bundle()
    return {"printer": _preset_summary(b.current_printer_preset()),
            "process": _preset_summary(b.current_process_preset()),
            "filaments": [dict(_preset_summary(p) or {}, slot=i + 1)
                          for i, p in enumerate(b.current_filament_presets())]}


@bridge_method("presets.list", doc="Presets de una categoría (process|printer|filament), con origen y compatibilidad.")
def _presets_list(params):
    coll = _collection(params.get("category", params.get("type", "process")))
    visible_only = bool(params.get("visible_only", True))
    items = []
    for i in range(_get(coll, "size", 0) or 0):
        p = coll.preset(i)
        if visible_only and (not _get(p, "is_visible", True) or not _get(p, "is_compatible", True)):
            continue
        items.append({"name": _get(p, "name"), "label": _get(p, "label"), "system": _get(p, "is_system"),
                      "user": _get(p, "is_user"), "bundle_id": _get(p, "bundle_id"),
                      "compatible": _get(p, "is_compatible"), "default": _get(p, "is_default")})
    return {"selected": _get(coll, "selected_preset_name"), "count": len(items), "presets": items}


@bridge_method("preset.get", doc="Configuración de un preset concreto (o del activo, con cambios sin guardar).")
def _preset_get(params):
    cat = _category(params.get("category", "process"))
    coll = _collection(cat)
    name = params.get("name")
    p = coll.find_preset(name) if name else coll.edited_preset()
    if p is None:
        raise BridgeError(f"Preset no encontrado: {name!r}")
    keys = params.get("keys")
    cfg = ({k: _jsonable(p.config_value(k)) for k in keys} if keys else _config_dict(p))
    return {"preset": _preset_summary(p), "config": cfg}


@bridge_method("config.get", doc="Valores resueltos de la configuración activa (proceso+filamento+impresora).")
def _config_get(params):
    keys = params.get("keys") or []
    keys = [keys] if isinstance(keys, str) else keys
    b = orca.host.preset_bundle()
    return {k: _jsonable(b.full_config_value(k)) for k in keys[:400]}


@bridge_method("config.search", doc="Busca claves de la configuración activa por expresión regular.")
def _config_search(params):
    pat = re.compile(params.get("pattern", ""), re.IGNORECASE)
    limit = int(params.get("limit", 100))
    b = orca.host.preset_bundle()
    keys = [k for k in b.full_config_keys() if pat.search(k)]
    return {"total": len(keys), "values": {k: _jsonable(b.full_config_value(k)) for k in keys[:limit]}}


@bridge_method("presets.diff", doc="Diferencias entre dos presets de la misma categoría.")
def _presets_diff(params):
    coll = _collection(params.get("category", params.get("type", "process")))
    a, b = coll.find_preset(params["a"]), coll.find_preset(params["b"])
    missing = [n for n, p in ((params["a"], a), (params["b"], b)) if p is None]
    if missing:
        raise BridgeError(f"Preset no encontrado: {missing}")
    ca, cb = _config_dict(a), _config_dict(b)
    diff = {k: {"a": ca.get(k), "b": cb.get(k)} for k in sorted(set(ca) | set(cb))
            if ca.get(k) != cb.get(k) and k not in META_KEYS}
    return {"a": params["a"], "b": params["b"], "count": len(diff), "differences": diff}


@bridge_method("ui.notify", doc="Aviso en la vista 3D. level: info|hint|warning|error.")
def _ui_notify(params):
    _notify(params.get("text", ""), params.get("level", "info"))
    return {"shown": True}


def _notify(text, level="info"):
    lv = orca.host.ui.NotificationLevel
    attr = {"info": "RegularNotificationLevel", "hint": "HintNotificationLevel",
            "warning": "WarningNotificationLevel", "error": "ErrorNotificationLevel"}.get(level, "RegularNotificationLevel")
    orca.host.ui.push_notification(getattr(lv, attr), f"[IA] {str(text)[:500]}")


# ─────────────────────────────────────────────────────────────────────────────
# Escritura de presets (hilo del script)
# ─────────────────────────────────────────────────────────────────────────────

def _editor(cat):
    pr = getattr(orca.host, "preset", None)
    ed = getattr(pr, cat, None) if pr is not None else None
    if ed is None or not hasattr(ed, "begin_update"):
        raise BridgeError("Esta versión de OrcaSlicer no permite editar presets desde plugins (requiere 2.5.0 o posterior).")
    return ed


def _begin(cat, preset_name="", slot=None):
    ed = _editor(cat)
    try:
        if cat == "filament":
            return ed.begin_update(int(slot or 1), preset_name or "")
        return ed.begin_update(preset_name or "")
    except RuntimeError as exc:
        raise BridgeError(f"OrcaSlicer no aplicó la selección: {exc} "
                          "(¿cancelaste el aviso de cambios sin guardar?)") from None
    except ValueError as exc:
        raise BridgeError(f"Preset o ranura no válidos: {exc}") from None


def _result_dict(res):
    return {k: _jsonable(_get(res, k)) for k in ("category", "target_name", "selection_changed", "slot_number",
                                                  "slot_assignment_changed", "shared_slots", "keys", "dirty")}


@bridge_method("preset.update", kind="worker", write=True,
               doc="Cambia ajustes del preset activo (o de preset_name / ranura de filamento) en vivo, como en la GUI. "
                   "Quedan como cambios sin guardar.")
async def _preset_update(params):
    cat = _category(params.get("category", "process"))
    settings = params.get("settings") or {}
    if not isinstance(settings, dict) or not settings:
        raise BridgeError("Indica settings como objeto {clave: valor}.")
    slot = params.get("slot")

    def current_values():
        b = orca.host.preset_bundle()
        if cat == "filament":
            presets = list(b.current_filament_presets())
            idx = int(slot or 1) - 1
            p = presets[idx] if 0 <= idx < len(presets) else None
        else:
            p = getattr(b, COLLECTION_ATTR[cat]).edited_preset()
        return {k: (p.config_value(k) if p is not None else None) for k in settings}
    current = await on_ui(current_values)
    unknown = [k for k, v in current.items() if v is None]
    if unknown and not params.get("preset_name"):
        raise BridgeError(f"Claves que no pertenecen a {cat}: {unknown}. "
                          "Usa search_config para encontrar el nombre correcto o cambia de categoría.")
    tx = _begin(cat, params.get("preset_name", ""), slot)
    try:
        for k, v in settings.items():
            tx.set(str(k), serialize_value(v, current.get(k)))
        res = tx.commit()
    except ValueError as exc:
        raise BridgeError(f"Valor o clave rechazados por OrcaSlicer: {exc}") from None
    except RuntimeError as exc:
        raise BridgeError(f"No se aplicó ningún cambio: {exc}") from None
    after = await on_ui(lambda: _preset_values(cat, slot, list(settings)))
    return {"applied": _result_dict(res), "values_now": after,
            "note": "Cambios sin guardar en el preset; guárdalos desde OrcaSlicer o con preset.create."}


def _preset_values(cat, slot, keys):
    b = orca.host.preset_bundle()
    if cat == "filament":
        presets = list(b.current_filament_presets())
        p = presets[int(slot or 1) - 1]
    else:
        p = getattr(b, COLLECTION_ATTR[cat]).edited_preset()
    return {k: p.config_value(k) for k in keys}


@bridge_method("preset.select", kind="worker", write=True,
               doc="Selecciona un preset (como en el desplegable). Para filamento indica la ranura.")
async def _preset_select(params):
    cat = _category(params.get("category", "process"))
    name = params.get("name")
    if not name:
        raise BridgeError("Indica name.")
    tx = _begin(cat, name, params.get("slot"))
    try:
        res = tx.commit()
        out = _result_dict(res)
    except Exception as exc:  # noqa: BLE001  (commit sin cambios: la selección ya se hizo)
        out = {"target_name": _get(tx, "target_name"), "selection_changed": _get(tx, "selection_changed"),
               "note": f"commit vacío: {exc}"}
    return {"selected": out, "now": await on_ui(lambda: _presets_current({}))}


# ── bundle local de presets ────────────────────────────────────────────────

def _bundle_dir(folder: str) -> str:
    return os.path.join(_data_dir(), "user", folder, "_local", BUNDLE_ID)


def _candidate_user_folders(preset_files: list[str]) -> list[str]:
    root = os.path.join(_data_dir(), "user")
    found = []
    for f in preset_files:
        parts = os.path.normpath(f or "").split(os.sep)
        if "user" in parts:
            i = len(parts) - 1 - parts[::-1].index("user")
            if i + 1 < len(parts) and parts[i + 1] not in found:
                found.append(parts[i + 1])
    others = []
    if os.path.isdir(root):
        others = sorted((d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d)) and d not in found),
                        key=lambda d: -os.path.getmtime(os.path.join(root, d)))
    out = found + others
    return out or ["default"]


def _write_bundle_metadata(folder: str):
    d = _bundle_dir(folder)
    os.makedirs(d, exist_ok=True)
    meta_path = os.path.join(d, "bundle_metadata.json")
    now = int(time.time())
    meta = {"id": BUNDLE_ID, "name": "MCP Bridge", "version": "1",
            "description": "Presets creados por agentes de IA a través de MCP Bridge",
            "author": "MCP Bridge", "imported_time": now, "updated_time": now}
    if os.path.exists(meta_path):
        try:
            with open(meta_path, encoding="utf-8") as fh:
                old = json.load(fh)
            meta["imported_time"] = old.get("imported_time", now)
        except ValueError:
            pass
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=4)
    return d


def _reload_bundle():
    pr = getattr(orca.host, "preset", None)
    try:
        pr.bundles.reload_local(BUNDLE_ID)
    except AttributeError:
        raise BridgeError("Esta versión de OrcaSlicer no puede recargar bundles locales (requiere 2.5.0).") from None
    except RuntimeError as exc:
        raise BridgeError(f"OrcaSlicer rechazó el bundle de presets: {exc}") from None


def _find_bundle_preset(cat: str, name: str):
    coll = _collection(cat)
    for i in range(_get(coll, "size", 0) or 0):
        p = coll.preset(i)
        if _get(p, "bundle_id") == BUNDLE_ID and (
                _get(p, "name", "").endswith("/" + name) or _get(p, "name") == name or _get(p, "alias") == name
                or _get(p, "label") == name):
            return p
    return None


@bridge_method("preset.create", kind="worker", write=True,
               doc="Crea (o reemplaza) un preset guardado en disco a partir de otro, con ajustes cambiados, "
                   "y opcionalmente lo selecciona.")
async def _preset_create(params):
    cat = _category(params.get("category", "process"))
    name = _safe_name(params.get("name", ""))
    overrides = params.get("settings") or {}
    base_name = params.get("base")

    def gather():
        coll = _collection(cat)
        if base_name:
            base = coll.find_preset(base_name)
            if base is None:
                raise BridgeError(f"Preset base no encontrado: {base_name!r}")
        else:
            base = coll.edited_preset()  # incluye los cambios sin guardar
        base_cfg = _config_dict(base)
        if _get(base, "is_system") or _get(base, "is_default"):
            # El padre es la versión guardada del preset de sistema; así se conservan los cambios sin guardar.
            parent_name = _get(base, "name")
            saved = coll.find_preset(parent_name)
            parent_cfg = _config_dict(saved) if saved is not None else base_cfg
        else:
            parent_name = base_cfg.get("inherits") or ""
            parent = coll.find_preset(parent_name) if parent_name else None
            parent_cfg = _config_dict(parent) if parent is not None else {}
            if parent is None:
                parent_name = ""
        files = []
        for c in COLLECTION_ATTR.values():
            col = getattr(orca.host.preset_bundle(), c)
            for i in range(_get(col, "size", 0) or 0):
                p = col.preset(i)
                if _get(p, "is_user") and _get(p, "file"):
                    files.append(_get(p, "file"))
        try:
            version = orca.host.app_info().get("version", "")
        except Exception:  # noqa: BLE001
            version = ""
        return {"base": _get(base, "name"), "base_cfg": base_cfg, "parent": parent_name,
                "parent_cfg": parent_cfg, "files": files, "version": version}
    g = await on_ui(gather)
    unknown = [k for k in overrides if k not in g["base_cfg"]]
    if unknown:
        raise BridgeError(f"Claves que no pertenecen a {cat}: {unknown}")

    doc = {"name": name, "from": "User", "is_custom_defined": "0",
           "version": _app_version_semver(g["version"]) , "inherits": g["parent"]}
    for k, v in g["base_cfg"].items():
        if k in META_KEYS or v is None:
            continue
        if not g["parent"] or g["parent_cfg"].get(k) != v:
            doc[k] = v
    for k, v in overrides.items():
        doc[k] = serialize_value(v, g["base_cfg"].get(k))
    sid = SETTINGS_ID_KEY[cat]
    doc[sid] = f'"{name}"' if cat == "filament" else name

    folders = _candidate_user_folders(g["files"])
    errors = []
    for folder in folders:
        d = _write_bundle_metadata(folder)
        sub = os.path.join(d, PRESET_DIRS[cat])
        os.makedirs(sub, exist_ok=True)
        path = os.path.join(sub, name + ".json")
        backup = None
        if os.path.exists(path):
            with open(path, "rb") as fh:
                backup = fh.read()
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=4, ensure_ascii=False)
        try:
            _reload_bundle()
            break
        except BridgeError as exc:
            errors.append(f"{folder}: {exc}")
            if backup is None:
                os.remove(path)
            else:
                with open(path, "wb") as fh:
                    fh.write(backup)
            if "does not exist" not in str(exc) and "directory" not in str(exc).lower():
                raise BridgeError(f"OrcaSlicer no aceptó el preset: {exc}") from None
    else:
        raise BridgeError("No pude registrar el bundle de presets: " + "; ".join(errors))

    created = await on_ui(lambda: _preset_summary(_find_bundle_preset(cat, name)))
    selected = None
    if params.get("select", True) and created:
        last = None
        for candidate in (created.get("label"), created.get("alias"), created.get("name"), name):
            if not candidate:
                continue
            try:
                selected = await _preset_select({"category": cat, "name": candidate, "slot": params.get("slot")})
                break
            except BridgeError as exc:
                last = exc
        if selected is None and last:
            selected = {"error": str(last)}
    return {"created": created, "file": path, "inherits": g["parent"] or None,
            "stored_keys": len(doc) - 6, "selected": selected}


@bridge_method("preset.delete", kind="worker", write=True,
               doc="Borra un preset creado por el puente (solo los del bundle MCP Bridge).")
async def _preset_delete(params):
    cat = _category(params.get("category", "process"))
    name = _safe_name(params.get("name", ""))
    files = await on_ui(lambda: [_get(p, "file") for p in [_find_bundle_preset(cat, name)] if p is not None])
    path = files[0] if files and files[0] else None
    if not path or not os.path.exists(path):
        raise BridgeError(f"{name!r} no es un preset del bundle MCP Bridge.")
    with open(path, "rb") as fh:
        backup = fh.read()
    os.remove(path)
    try:
        _reload_bundle()
    except BridgeError:
        with open(path, "wb") as fh:
            fh.write(backup)
        raise
    return {"deleted": name, "file": path}


@bridge_method("bundles.list", kind="worker", doc="Bundles locales de presets.")
def _bundles_list(_params):
    return {"bundles": list(orca.host.preset.bundles.list_ids())}


@bridge_method("bundles.reload", kind="worker", write=True, doc="Recarga un bundle local desde disco.")
def _bundles_reload(params):
    try:
        orca.host.preset.bundles.reload_local(params["bundle_id"])
    except RuntimeError as exc:
        raise BridgeError(str(exc)) from None
    return {"reloaded": params["bundle_id"]}


# ─────────────────────────────────────────────────────────────────────────────
# Modelo: 3MF + CLI + reapertura en la ventana abierta
# ─────────────────────────────────────────────────────────────────────────────

def _work_dir(kind: str) -> str:
    d = os.path.join(STATE.storage, "work", f"{kind}_{time.strftime('%Y%m%d_%H%M%S')}_{secrets.token_hex(2)}")
    os.makedirs(d, exist_ok=True)
    return d


def _cleanup_work(keep: int = 15):
    root = os.path.join(STATE.storage, "work")
    if not os.path.isdir(root):
        return
    dirs = sorted((os.path.join(root, d) for d in os.listdir(root)), key=os.path.getmtime, reverse=True)
    for d in dirs[keep:]:
        shutil.rmtree(d, ignore_errors=True)


async def _export_current(dest_dir: str) -> str:
    def export():
        plater = orca.host.plater()
        stem = os.path.splitext(os.path.basename(_get(plater, "project_path") or ""))[0] or "proyecto"
        stem = re.sub(r"[^\w.-]+", "_", stem)[:60]
        path = os.path.join(dest_dir, stem + ".3mf")
        plater.export_3mf_copy(path)
        return path
    try:
        return await on_ui(export, timeout=120)
    except RuntimeError as exc:
        raise BridgeError(f"No pude exportar el proyecto: {exc}") from None


def _publish_project(path: str) -> str:
    """Copia un 3MF resultado a la carpeta 'proyectos' (no se limpia) antes de abrirlo en OrcaSlicer."""
    d = os.path.join(STATE.storage, "proyectos")
    os.makedirs(d, exist_ok=True)
    stem = os.path.splitext(os.path.basename(path))[0]
    stem = re.sub(r"(_IA(_\d{8}_\d{6})?)+$", "", stem) or "proyecto"
    dest = os.path.join(d, f"{stem}_IA_{time.strftime('%Y%m%d_%H%M%S')}.3mf")
    shutil.copyfile(path, dest)
    return dest


def _exe() -> str:
    if not STATE.exe:
        raise BridgeError("No se encontró el ejecutable de OrcaSlicer; indícalo en la configuración del plugin (orca_executable).")
    return STATE.exe


def escape_strings_cstyle(strs: list[str]) -> str:
    """Igual que Slic3r::escape_strings_cstyle (libslic3r/Config.cpp): formato de los mensajes entre instancias."""
    out = []
    for s in strs:
        quote = (len(strs) == 1 and s == "") or any(c in s for c in ' \t;\\"\r\n')
        if quote:
            s = s.replace("\\", "\\\\").replace('"', '\\"').replace("\r", "\\r").replace("\n", "\\n")
            s = f'"{s}"'
        out.append(s)
    return ";".join(out)


def _own_main_windows() -> list[int]:
    """Ventanas principales de ESTE proceso de OrcaSlicer (clase wxWindowNR con las propiedades Instance_Hash_*,
    que es como OrcaSlicer reconoce sus ventanas en InstanceCheck.cpp)."""
    import ctypes
    from ctypes import wintypes
    user32 = ctypes.windll.user32
    user32.GetPropW.restype = wintypes.HANDLE
    user32.GetPropW.argtypes = [wintypes.HWND, wintypes.LPCWSTR]
    pid = os.getpid()
    tagged, untagged = [], []
    proto = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    def cb(hwnd, _lp):
        owner = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
        if owner.value != pid:
            return True
        cls = ctypes.create_unicode_buffer(256)
        if not user32.GetClassNameW(hwnd, cls, 256) or cls.value != "wxWindowNR":
            return True
        if user32.GetPropW(hwnd, "Instance_Hash_Minor") or user32.GetPropW(hwnd, "Instance_Hash_Major"):
            tagged.append(hwnd)
        elif user32.IsWindowVisible(hwnd):
            untagged.append(hwnd)
        return True

    user32.EnumWindows(proto(cb), 0)
    return tagged or untagged


def _load_in_own_window(path: str) -> bool:
    """Entrega el archivo a la ventana abierta igual que lo haría una segunda instancia de OrcaSlicer: WM_COPYDATA
    con dwData=1 y la línea de órdenes escapada. OrcaSlicer lo atiende SIEMPRE (GUI_App.cpp), tenga o no activada
    la preferencia de instancia única, y llama a Plater::load_files en la misma ventana."""
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        from ctypes import wintypes
        hwnds = _own_main_windows()
        if not hwnds:
            return False

        class COPYDATASTRUCT(ctypes.Structure):
            _fields_ = [("dwData", ctypes.c_size_t), ("cbData", wintypes.DWORD), ("lpData", ctypes.c_void_p)]

        msg = escape_strings_cstyle([STATE.exe or "orca-slicer.exe", path])
        buf = ctypes.create_unicode_buffer(msg)
        cds = COPYDATASTRUCT(1, ctypes.sizeof(buf), ctypes.cast(buf, ctypes.c_void_p))
        res = ctypes.c_size_t(0)
        user32 = ctypes.windll.user32
        user32.SendMessageTimeoutW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM,
                                               wintypes.UINT, wintypes.UINT, ctypes.POINTER(ctypes.c_size_t)]
        ok = user32.SendMessageTimeoutW(hwnds[0], 0x004A, 0, ctypes.addressof(cds), 0x0002, 15000, ctypes.byref(res))
        STATE.log(f"WM_COPYDATA a la ventana {hwnds[0]:#x}: {'ok' if ok else 'fallo'}")
        return bool(ok)
    except Exception as e:  # noqa: BLE001
        STATE.log(f"WM_COPYDATA no disponible: {e}")
        return False


async def _reopen(path: str):
    if _load_in_own_window(path):
        how = "ventana"
    else:
        # Respaldo (Linux/macOS o si falla lo anterior): lanzar OrcaSlicer con la ruta. OrcaSlicer 2.5 rechaza
        # "--single-instance"; con la preferencia de instancia única el archivo llega a la ventana abierta.
        res = await run_process([_exe(), path], os.path.dirname(path), timeout=60)
        if res["returncode"] not in (0, None, -1, 255):
            raise BridgeError(f"OrcaSlicer no aceptó abrir el archivo (código {res['returncode']}): {res['stderr'][-400:]}")
        how = "proceso"
    try:
        await on_ui(lambda: _notify("Proyecto modificado por IA: confirma la carga. Recuerda 'Guardar como' "
                                    "en tu carpeta si quieres conservarlo.", "hint"), timeout=5)
    except BridgeError:
        pass
    note = ("Enviado a la ventana abierta de OrcaSlicer; puede pedir confirmar la carga del proyecto."
            if how == "ventana" else
            "Abierto lanzando OrcaSlicer: con 'Permitir solo una instancia' llega a la ventana abierta; si no, "
            "se abre en otra ventana.")
    return {"path": path, "via": how, "note": note}


@bridge_method("project.inspect", kind="worker",
               doc="Lista los objetos tal como se guardan en el 3MF (índices, instancias, piezas, ajustes por objeto).")
async def _project_inspect(params):
    path = params.get("path")
    if not path:
        path = await _export_current(_work_dir("inspect"))
    p = Project3MF(path)
    keys = params.get("project_keys") or []
    proj = {k: p.project.get(k) for k in keys} if (p.project and keys) else None
    return {"path": path, "objects": p.objects(), "project_settings": proj,
            "has_project_settings": p.project is not None}


# ── aplicación en directo (OrcaSlicer con orca.host.edit) ────────────────────
# Con la API de escritura de MCPOrcaSlicer, model.edit no recarga el proyecto: aplica las operaciones sobre una
# copia exportada con Project3MF (el mismo código probado) y lleva a la sesión abierta solo las diferencias, cada
# una como un paso de deshacer de OrcaSlicer. Sin esa API se recarga el proyecto como siempre.
LIVE_UNSUPPORTED_OPS = {"set_brim_ears", "set_plate", "set_project_info"}
# Funciones de orca.host.edit que hacen falta para llevar en directo cada operación.
LIVE_OP_REQUIRES = {"set_brim_ears": "set_brim_ears", "set_plate": "set_plate", "set_project_info": "set_project_info"}
PROJECT_INFO_TAGS = {"title": "Title", "designer": "Designer", "description": "Description", "license": "License",
                     "copyright": "Copyright", "origin": "Origin"}
PLATE_LIVE_KEYS = {"plater_name": "name", "bed_type": "bed_type", "print_sequence": "print_sequence",
                   "spiral_mode": "spiral_mode", "locked": "locked"}


def _live_unsupported(ed) -> set:
    """Operaciones que la API en directo de esta versión no cubre (se recarga el proyecto)."""
    return {op for op in LIVE_UNSUPPORTED_OPS if not hasattr(ed, LIVE_OP_REQUIRES.get(op, op))}
STRING_VECTOR_KEYS = {"filament_colour", "filament_multi_colour", "filament_colour_type"}
PROJECT_LIVE_KEYS = ("filament_colour", "flush_volumes_matrix", "flush_volumes_vector", "flush_multiplier")


def _brim_ears(p: "Project3MF") -> dict:
    """Orejas de borde por objeto (índice 1-based del 3MF) → [[x, y, z, radio], …] en coordenadas del objeto."""
    out = {}
    for ln in p.data.get(BRIM_EARS_FILE, b"").decode("utf-8").splitlines()[1:]:
        if not ln.startswith("object_id=") or "|" not in ln:
            continue
        key, _, rest = ln.partition("|")
        nums = [float(x) for x in rest.split()]
        out[key.split("=", 1)[1]] = [nums[i:i + 4] for i in range(0, len(nums) - 3, 4)]
    return out


def _plate_settings(p: "Project3MF") -> list:
    return [{k: p._meta(pl, k) for k in PLATE_LIVE_KEYS} for pl in p.settings.findall("plate")]


def _project_info(p: "Project3MF") -> dict:
    import html
    out = {}
    for key, tag in PROJECT_INFO_TAGS.items():
        m = re.search(r'<metadata name="%s">(.*?)</metadata>' % tag, p.model, re.S)
        out[key] = html.unescape(m.group(1)) if m else ""
    return out


def live_edit_api():
    host = getattr(orca, "host", None)
    return getattr(host, "edit", None) if host is not None else None


def _flat16(tr: Affine) -> list:
    A, t = tr.A, tr.t
    return [A[0][0], A[0][1], A[0][2], t[0], A[1][0], A[1][1], A[1][2], t[1],
            A[2][0], A[2][1], A[2][2], t[2], 0.0, 0.0, 0.0, 1.0]


def _same_tr(a: str | None, b: str | None) -> bool:
    x, y = Affine.parse(a), Affine.parse(b)
    return all(abs(x.A[r][c] - y.A[r][c]) < 1e-7 for r in range(3) for c in range(3)) and \
        all(abs(x.t[k] - y.t[k]) < 1e-6 for k in range(3))


def _object_meta(p: "Project3MF", oid: str) -> dict:
    node = p._settings_object(oid)
    return {m.get("key"): m.get("value") for m in node.findall("metadata")} if node is not None else {}


def _part_info(p: "Project3MF", obj: dict) -> list:
    """Por pieza: id del subobjeto, transformación, tipo, nombre, ajustes y malla (V, T, attrs)."""
    node = p._settings_object(obj["object_id"])
    pnodes = node.findall("part") if node is not None else []
    out = []
    for i, comp in enumerate(p._components(obj["object_id"])):
        a = _parse_attrs(comp)
        pn = next((x for x in pnodes if x.get("id") == a.get("objectid")), pnodes[i] if i < len(pnodes) else None)
        meta = {m.get("key"): m.get("value") for m in (pn.findall("metadata") if pn is not None else [])}
        _path, _text, _m, V, T, A = p._read_part_mesh(obj, i)
        out.append({"sub": a.get("objectid"), "transform": a.get("transform"),
                    "subtype": pn.get("subtype", "normal_part") if pn is not None else "normal_part",
                    "name": meta.pop("name", ""), "settings": {k: v for k, v in meta.items()
                                                                if k not in ("matrix", "source_object_id",
                                                                             "source_volume_id", "source_offset_x",
                                                                             "source_offset_y", "source_offset_z",
                                                                             "mesh_stat")},
                    "V": V, "T": T, "A": A})
    return out


def _paint_layers(A: list) -> dict:
    return {layer: [a.get(attr, "") for a in A] for layer, attr in PAINT_LAYERS.items()
            if any(attr in a for a in A)}


def _config_diff(old: dict, new: dict, ignore=("name",)) -> tuple[dict, list]:
    values = {k: str(v) for k, v in new.items() if k not in ignore and old.get(k) != v}
    erase = [k for k in old if k not in ignore and k not in new]
    return values, erase


async def _live_volume_ids(L: int) -> list:
    return await on_ui(lambda: [v.id() for v in orca.host.model().object(L).volumes()], timeout=10)


async def _apply_live(p0: "Project3MF", p1: "Project3MF", work: str) -> list:
    """Lleva a la sesión abierta las diferencias entre p0 (proyecto exportado) y p1 (tras las operaciones)."""
    ed = live_edit_api()
    done = []
    objs0 = {o["object_id"]: o for o in p0.objects()}
    objs1 = {o["object_id"]: o for o in p1.objects()}
    live_idx = {oid: p0._model_index(oid) for oid in objs0}

    # ── filamentos y ajustes del proyecto ──
    P0, P1 = p0.project or {}, p1.project or {}
    c0, c1 = list(P0.get("filament_colour") or []), list(P1.get("filament_colour") or [])
    for col in c1[len(c0):]:
        ed.add_filament(str(col))
        done.append(f"filamento añadido ({col})")
    proj = {k: P1[k] for k in PROJECT_LIVE_KEYS if k in P1 and P1.get(k) != P0.get(k)}
    if len(c1) > len(c0):
        proj["filament_colour"] = c1
    if proj:
        # Serialización de OrcaSlicer: los vectores de texto (colores) van con ";", los numéricos con ",".
        ed.set_project_config({k: ((";" if k in STRING_VECTOR_KEYS else ",").join(str(x) for x in v)
                                   if isinstance(v, list) else str(v)) for k, v in proj.items()})
        done.append(f"ajustes del proyecto: {sorted(proj)}")
    other = {k for k in set(P0) | set(P1) if P0.get(k) != P1.get(k)} - set(PROJECT_LIVE_KEYS) - set(FILAMENT_VECTOR_KEYS) \
        - {"filament_self_index", "different_settings_to_system", "inherits_group", "compatible_machine_expression_group"}
    for key in sorted(other):
        val = P1.get(key)
        sval = ";".join(str(x) for x in val) if isinstance(val, list) else str(val)
        for cat in ("process", "printer"):
            try:
                tx = _begin(cat)
                tx.set(key, sval)
                tx.commit()
                done.append(f"{key} = {sval} ({'proceso' if cat == 'process' else 'impresora'})")
                break
            except Exception:  # noqa: BLE001 — la clave no es de esa categoría
                continue

    # ── objetos que siguen ──
    for oid, o1 in objs1.items():
        if oid not in objs0:
            continue
        o0, L = objs0[oid], live_idx[oid]
        if o0["name"] != o1["name"]:
            ed.rename(L, -1, o1["name"])
            done.append(f"{o0['name']}: renombrado a {o1['name']}")
        m0, m1 = _object_meta(p0, oid), _object_meta(p1, oid)
        values, erase = _config_diff(m0, m1)
        if values or erase:
            ed.set_config(L, -1, values, erase)
            done.append(f"{o1['name']}: ajustes {sorted(values) + ['-' + k for k in erase]}")
        # instancias
        n0, n1 = len(o0["instances"]), len(o1["instances"])
        for k in range(n0 - 1, n1 - 1, -1):
            ed.delete_instance(L, k)
        for k in range(n0, n1):
            t = Affine.parse(o1["instances"][k]["transform"]).t
            ed.add_instance(L, [t[0], t[1], t[2]])
        for k in range(n1):
            i1 = o1["instances"][k]
            i0 = o0["instances"][k] if k < n0 else None
            if i0 is None or not _same_tr(i0["transform"], i1["transform"]):
                ed.set_instance_matrix(L, k, _flat16(Affine.parse(i1["transform"])))
            if i0 is None or bool(i0.get("printable", True)) != bool(i1.get("printable", True)):
                ed.set_printable(L, k, bool(i1.get("printable", True)))
        if n0 != n1 or any(not _same_tr(a["transform"], b["transform"]) for a, b in zip(o0["instances"], o1["instances"])):
            done.append(f"{o1['name']}: {n1} instancia(s) colocadas")
        # piezas
        parts0, parts1 = _part_info(p0, o0), _part_info(p1, o1)
        ids = await _live_volume_ids(L)
        by_sub0 = {pt["sub"]: (i, pt) for i, pt in enumerate(parts0)}
        cur = {pt["sub"]: ids[i] for i, pt in enumerate(parts0) if i < len(ids)}   # pieza → id de ModelVolume
        subs1 = {pt["sub"] for pt in parts1}

        async def index_of(sub):
            return (await _live_volume_ids(L)).index(cur[sub])
        for pt in parts1:
            if pt["sub"] not in by_sub0:
                continue
            _i0, q = by_sub0[pt["sub"]]
            j = await index_of(pt["sub"])
            if [list(map(float, v)) for v in q["V"]] != [list(map(float, v)) for v in pt["V"]] or q["T"] != pt["T"]:
                ed.set_volume_mesh(L, j, pt["V"], [list(t) for t in pt["T"]], _paint_layers(pt["A"]))
                cur[pt["sub"]] = (await _live_volume_ids(L))[j]      # OrcaSlicer da un id nuevo a la pieza
                done.append(f"{o1['name']}: malla de la pieza '{pt['name']}' ({len(pt['T'])} triángulos)")
            else:
                l0, l1 = _paint_layers(q["A"]), _paint_layers(pt["A"])
                for layer in set(l0) | set(l1):
                    if l0.get(layer) != l1.get(layer):
                        codes = l1.get(layer) or [""] * len(pt["T"])
                        ed.set_paint(L, j, layer, codes)
                        done.append(f"{o1['name']}: pintura '{layer}' ({sum(1 for c in codes if c)} triángulos)")
            if not _same_tr(q["transform"], pt["transform"]):
                ed.set_volume_matrix(L, j, _flat16(Affine.parse(pt["transform"])))
                done.append(f"{o1['name']}: pieza '{pt['name']}' transformada")
            values, erase = _config_diff(q["settings"], pt["settings"], ignore=())
            if values or erase:
                ed.set_config(L, j, values, erase)
                done.append(f"{o1['name']}: ajustes de la pieza '{pt['name']}'")
            if q["name"] != pt["name"] and pt["name"]:
                ed.rename(L, j, pt["name"])
        for sub in [pt["sub"] for pt in parts0 if pt["sub"] not in subs1]:
            ed.delete_volume(L, await index_of(sub))
            done.append(f"{o1['name']}: pieza eliminada")
        for pt in parts1:
            if pt["sub"] in by_sub0 and by_sub0[pt["sub"]][1]["subtype"] != pt["subtype"]:
                ed.set_volume_type(L, await index_of(pt["sub"]), _PART_TYPE_NAMES.get(pt["subtype"], "part"))
                done.append(f"{o1['name']}: pieza '{pt['name']}' ahora es {_PART_TYPE_NAMES.get(pt['subtype'])}")
        for pt in parts1:
            if pt["sub"] in by_sub0:
                continue
            ptype = _PART_TYPE_NAMES.get(pt["subtype"], "part")
            new_i = ed.add_volume(L, pt["V"], [list(t) for t in pt["T"]], ptype, pt["name"])
            ed.set_volume_matrix(L, new_i, _flat16(Affine.parse(pt["transform"])))
            if pt["settings"]:
                ed.set_config(L, new_i, {k: str(v) for k, v in pt["settings"].items()}, [])
            for layer, codes in _paint_layers(pt["A"]).items():
                ed.set_paint(L, new_i, layer, codes)
            done.append(f"{o1['name']}: añadida pieza '{pt['name']}' ({ptype})")

    # ── capas (rangos, capa variable) por objeto que sigue ──
    li0, li1 = p0.layer_info(), p1.layer_info()
    for oid, o1 in objs1.items():
        if oid not in objs0:
            continue
        L = live_idx[oid]
        k0, k1 = str(p0._model_index(oid) + 1), str(p1._model_index(oid) + 1)
        r0 = li0["layer_ranges_by_object"].get(k0, [])
        r1 = li1["layer_ranges_by_object"].get(k1, [])
        if r0 != r1:
            ed.set_layer_ranges(L, [(r["min_z"], r["max_z"], r["settings"]) for r in r1])
            done.append(f"{o1['name']}: {len(r1)} rango(s) de altura")
        h0 = li0["variable_layer_height_by_object"].get(k0, "")
        h1 = li1["variable_layer_height_by_object"].get(k1, "")
        if h0 != h1:
            ed.set_layer_height_profile(L, [float(x) for x in h1.split(";") if x] if h1 else [])
            done.append(f"{o1['name']}: altura de capa variable")
    g0, g1 = li0["layer_gcodes"], li1["layer_gcodes"]
    if g0 != g1:
        plates = sorted({g["plate"] for g in g0 + g1})
        n_fil = len(c1 or [1])
        for pl in plates:
            items = [g for g in g1 if g["plate"] == pl]
            kinds = {"0": "color_change", "1": "pause", "2": "tool_change", "3": "template", "4": "custom"}
            ed.set_layer_gcodes(pl - 1, [{"z": float(g["top_z"]), "type": kinds.get(g.get("type"), "custom"),
                                          "extruder": int(g.get("extruder", 1)), "color": g.get("color", ""),
                                          "extra": g.get("extra", "")} for g in items],
                                "SingleExtruder" if n_fil <= 1 else "MultiAsSingle")
            done.append(f"placa {pl}: {len(items)} G-code por capa")

    # ── orejas de borde ──
    if hasattr(ed, "set_brim_ears"):
        e0, e1 = _brim_ears(p0), _brim_ears(p1)
        for oid, o1 in objs1.items():
            if oid not in objs0:
                continue
            a = e0.get(str(p0._model_index(oid) + 1), [])
            b = e1.get(str(p1._model_index(oid) + 1), [])
            if a != b:
                ed.set_brim_ears(live_idx[oid], b)
                done.append(f"{o1['name']}: {len(b)} orejas de borde")

    # ── placas ──
    if hasattr(ed, "set_plate"):
        pl0, pl1 = _plate_settings(p0), _plate_settings(p1)
        for k, (a, b) in enumerate(zip(pl0, pl1)):
            changed = {PLATE_LIVE_KEYS[key]: b.get(key) for key in PLATE_LIVE_KEYS if a.get(key) != b.get(key)}
            if not changed:
                continue
            kw = {}
            for name, val in changed.items():
                if name == "locked":
                    kw[name] = str(val).lower() in ("1", "true")
                elif name == "spiral_mode":
                    kw[name] = "default" if val in (None, "") else ("1" if str(val).lower() in ("1", "true") else "0")
                elif name == "print_sequence":
                    kw[name] = val or "default"
                elif val is not None:
                    kw[name] = str(val)
            ed.set_plate(k, **kw)
            done.append(f"placa {k + 1}: {sorted(kw)}")

    # ── información del proyecto ──
    if hasattr(ed, "set_project_info"):
        i0, i1 = _project_info(p0), _project_info(p1)
        info = {k: v for k, v in i1.items() if i0.get(k) != v}
        if info:
            ed.set_project_info(info)
            done.append(f"información del proyecto: {sorted(info)}")

    # ── objetos nuevos: se importan sin diálogos y se colocan donde los dejó la edición ──
    new_oids = [oid for oid in objs1 if oid not in objs0]
    if new_oids:
        path = os.path.join(work, "nuevos_objetos.3mf")
        full = os.path.join(work, "edicion_completa.3mf")
        p1.save(full)
        keep = Project3MF(full)
        for oid in [o for o in objs1 if o not in new_oids]:
            keep._delete_object(oid)
        keep.save(path)
        created = ed.import_files([path])
        for oid, L in zip(new_oids, created):
            o1 = objs1[oid]
            ed.set_instance_matrix(L, 0, _flat16(Affine.parse(o1["instances"][0]["transform"])))
            for k in range(1, len(o1["instances"])):
                t = Affine.parse(o1["instances"][k]["transform"]).t
                ed.add_instance(L, [t[0], t[1], t[2]])
                ed.set_instance_matrix(L, k, _flat16(Affine.parse(o1["instances"][k]["transform"])))
            done.append(f"objeto nuevo '{o1['name']}'")

    # ── objetos borrados (al final y de mayor a menor índice) ──
    for oid in sorted((o for o in objs0 if o not in objs1), key=lambda o: -live_idx[o]):
        ed.delete_object(live_idx[oid])
        done.append(f"objeto '{objs0[oid]['name']}' eliminado")
    return done


@bridge_method("live.call", kind="worker", write=True,
               doc="Llama a una función de orca.host.edit (API de escritura en directo de MCPOrcaSlicer): "
                   "{function, args: [...], kwargs: {...}}. Sin function lista las disponibles.")
async def _live_call(params):
    ed = live_edit_api()
    if ed is None:
        raise BridgeError("Este OrcaSlicer no tiene la API de escritura en directo (orca.host.edit); "
                          "hace falta la versión MCPOrcaSlicer.")
    fn = params.get("function")
    names = sorted(n for n in dir(ed) if not n.startswith("_"))
    if not fn:
        return {"functions": {n: (getattr(ed, n).__doc__ or "").strip().splitlines()[-1:] for n in names}}
    if fn not in names:
        raise BridgeError(f"Función desconocida {fn!r}. Disponibles: {', '.join(names)}")
    try:
        res = getattr(ed, fn)(*(params.get("args") or []), **(params.get("kwargs") or {}))
    except (ValueError, IndexError, RuntimeError, TypeError) as exc:
        raise BridgeError(f"{fn}: {exc}") from None
    return {"function": fn, "result": _jsonable(res)}


@bridge_method("model.edit", kind="worker", write=True,
               doc="Modifica el modelo (mover, rotar, escalar, reflejar, apoyar, borrar, renombrar, pintar topos, ajustes por "
                   "objeto/pieza, extrusor, imprimible, ajustes de proyecto) y lo reabre en OrcaSlicer.")
async def _model_edit(params):
    ops = params.get("operations") or []
    if not ops:
        raise BridgeError("Indica operations: lista de operaciones.")
    async with STATE.write_lock:
        work = _work_dir("edit")
        src = params.get("path") or await _export_current(work)
        ops = [dict(op) for op in ops]
        await _prepare_mesh_files(ops, work)
        ed = live_edit_api()
        live = (ed is not None and params.get("live", True) and not params.get("path")
                and not any(op.get("op") in _live_unsupported(ed) for op in ops))
        p0 = Project3MF(src) if live else None
        p = Project3MF(src)
        log = [p.apply(op) for op in ops]
        out = os.path.join(work, os.path.splitext(os.path.basename(src))[0] + "_IA.3mf")
        p.save(out)
        result = {"changes": log, "objects": p.objects()}
        if live:
            grouped = hasattr(ed, "begin_undo_group")
            if grouped:
                ed.begin_undo_group(str(params.get("undo_name") or "MCP: edición del modelo"))
            try:
                result["applied_live"] = await _apply_live(p0, p, work)
            except (ValueError, IndexError, RuntimeError, TypeError) as exc:
                raise BridgeError(f"OrcaSlicer rechazó un cambio en directo: {exc}. Lo aplicado hasta ahí se puede "
                                  "deshacer con Ctrl+Z (o live.call undo).") from None
            finally:
                if grouped:
                    ed.end_undo_group()
            result["mode"] = ("en directo (toda la edición es un solo paso de deshacer)" if grouped
                              else "en directo (cada cambio es un paso de deshacer)")
            _cleanup_work()
            return result
        if params.get("reopen", True):
            out = _publish_project(out)
            result["reopen"] = await _reopen(out)
            if result["reopen"].get("via") == "ventana" and params.get("sync", True):
                result["sync"] = await _sync_after_load(out, (p.project or {}).get("filament_colour") or [],
                                                        float(params.get("sync_timeout", 180)))
        result["output"] = out
        _cleanup_work()
        return result


async def _sync_after_load(path: str, colours: list, timeout: float) -> dict:
    """Espera a que OrcaSlicer cargue el proyecto enviado. Si el usuario (o la preferencia de carga) lo importa
    solo como geometría, OrcaSlicer no aplica los ajustes del proyecto: se aplican aquí los colores de filamento
    en vivo y se avisa de que el objeto anterior sigue en la placa."""
    norm = os.path.normcase(os.path.abspath(path))
    deadline = time.monotonic() + timeout
    scene = None
    while time.monotonic() < deadline:
        await asyncio.sleep(1.0)
        try:
            scene = await on_ui(lambda: _scene_get({"mesh_stats": False}), timeout=10)
        except BridgeError:
            continue
        objs = scene.get("objects") or []
        if any(os.path.normcase(os.path.abspath(o.get("input_file") or "")) == norm for o in objs):
            break
    else:
        return {"loaded": False, "note": "OrcaSlicer no ha cargado el proyecto todavía (¿diálogo sin confirmar?)."}
    objs = scene.get("objects") or []
    others = [o["name"] for o in objs if os.path.normcase(os.path.abspath(o.get("input_file") or "")) != norm]
    if not others:
        return {"loaded": True, "mode": "proyecto"}
    fixed = {}
    for i, col in enumerate(colours, start=1):
        try:
            cur = await on_ui(lambda i=i: _preset_values("filament", i, ["filament_colour"]), timeout=10)
        except (BridgeError, IndexError):
            continue
        if str(cur.get("filament_colour", "")).upper() != str(col).upper():
            try:
                tx = _begin("filament", "", i)
                tx.set("filament_colour", str(col))
                tx.commit()
                fixed[i] = col
            except Exception as exc:  # noqa: BLE001
                fixed[i] = f"error: {exc}"
    try:
        await on_ui(lambda: _notify("Se importó como geometría: el objeto anterior sigue en la placa. Para sustituirlo, "
                                    "elige 'Abrir como proyecto' al cargar.", "warning"), timeout=5)
    except BridgeError:
        pass
    return {"loaded": True, "mode": "geometria", "objects_still_on_plate": others, "filament_colours_applied": fixed,
            "note": "OrcaSlicer lo importó solo como geometría (sin ajustes del proyecto): los objetos anteriores "
                    "siguen en la placa. Se han aplicado en vivo los colores de filamento. Para que sustituya el "
                    "proyecto, elige 'Abrir como proyecto' en el diálogo de carga."}


_CLI_FLAGS = {
    "arrange": lambda v: ["--arrange", "1" if v else "0"],
    "orient": lambda v: ["--orient", "1" if v else "0"],
    "allow_rotations": lambda v: ["--allow-rotations"] if v else [],
    "ensure_on_bed": lambda v: ["--ensure-on-bed"] if v else [],
    "repair": lambda v: ["--repair"] if v else [],
    "ground_largest_face": lambda v: ["--ground-largest-face"] if v else [],
    "rotate_z": lambda v: ["--rotate", str(float(v))],
    "rotate_x": lambda v: ["--rotate-x", str(float(v))],
    "rotate_y": lambda v: ["--rotate-y", str(float(v))],
    "scale": lambda v: ["--scale", str(float(v))],
    "scale_to_fit": lambda v: ["--scale-to-fit", ",".join(str(float(x)) for x in v)],
    "clone_objects": lambda v: ["--clone-objects", ",".join(str(int(x)) for x in v)],
    "ground_face_normal": lambda v: ["--ground-face-normal", ",".join(str(float(x)) for x in v)],
    "ground_face_point": lambda v: ["--ground-face-point", ",".join(str(float(x)) for x in v)],
    "convert_unit": lambda v: ["--convert-unit"] if v else [],
}


@bridge_method("model.cli", kind="worker", write=True,
               doc="Aplica herramientas de OrcaSlicer al proyecto (organizar, orientar, reparar, apoyar sobre la cara "
                   "mayor, rotar/escalar todo, cortar, clonar) con la CLI y lo reabre.")
async def _model_cli(params):
    actions = params.get("actions") or {}
    native = {"arrange", "orient"}
    ed = live_edit_api()
    if ed is not None and params.get("live", True) and actions and set(actions) <= native and all(actions.values()):
        if actions.get("orient"):
            ed.orient()
        if actions.get("arrange"):
            ed.arrange()
        return {"mode": "en directo (herramientas nativas de OrcaSlicer)", "actions": sorted(actions)}
    unknown = [k for k in actions if k not in _CLI_FLAGS]
    if unknown or not actions:
        raise BridgeError(f"Acciones no válidas: {unknown}. Disponibles: {sorted(_CLI_FLAGS)}")
    async with STATE.write_lock:
        work = _work_dir("cli")
        src = await _export_current(work)
        out_name = os.path.splitext(os.path.basename(src))[0] + "_IA.3mf"
        args = [_exe()]
        for k, v in actions.items():
            args += _CLI_FLAGS[k](v)
        args += ["--outputdir", work, "--export-3mf", out_name, src]
        res = await run_process(args, work, timeout=float(STATE.cfg.get("cli_timeout_s", 900)))
        out = os.path.join(work, out_name)
        if res["returncode"] != 0 or not os.path.exists(out):
            raise BridgeError(f"La CLI de OrcaSlicer falló (código {res['returncode']}): "
                              f"{(res['stderr'] or res['stdout'])[-800:]}")
        result = {"actions": actions, "cli_seconds": res["seconds"], "objects": Project3MF(out).objects()}
        if params.get("reopen", True):
            out = _publish_project(out)
            result["reopen"] = await _reopen(out)
        result["output"] = out
        _cleanup_work()
        return result


MESH_FORMATS_DIRECT = (".stl", ".obj", ".3mf")


async def _prepare_mesh_files(ops: list, work: str):
    """add_object/add_part con file: STEP y demás formatos (o rutas que el plugin no puede leer) se convierten
    primero a 3MF con la CLI de OrcaSlicer, que lee cualquier formato que admita la ventana."""
    for op in ops:
        f = op.get("file")
        if not f or op.get("op") not in ("add_object", "add_part"):
            continue
        if not os.path.exists(f):
            raise BridgeError(f"El archivo no existe: {f!r}")
        ext = os.path.splitext(f)[1].lower()
        readable = True
        try:
            with open(f, "rb"):
                pass
        except OSError:
            readable = False
        if ext in MESH_FORMATS_DIRECT and readable:
            continue
        name = f"import_{len(os.listdir(work))}.3mf"
        res = await run_process([_exe(), "--outputdir", work, "--export-3mf", name, f], work, timeout=300)
        out = os.path.join(work, name)
        if not os.path.exists(out):
            raise BridgeError(f"OrcaSlicer no pudo convertir {os.path.basename(f)} (código {res['returncode']}): "
                              f"{(res['stderr'] or res['stdout'])[-500:]}")
        op["_mesh_file"] = out


OPERATIONS_DOC = {
    "translate": "Mover: {object, instance?, x, y, z} (mm, desplazamiento).",
    "set_position": "Colocar: {object, instance?, x?, y?, z?, drop_to_bed?}.",
    "rotate": "Rotar: {object, instance?, axis x|y|z, degrees, drop_to_bed? (true)}.",
    "scale": "Escalar: {object, instance?, factor número o [fx,fy,fz]}.",
    "mirror": "Reflejar: {object, instance?, axis}.",
    "drop_to_bed": "Apoyar en la cama: {object, instance?}.",
    "set_printable": "{object, instance?, printable true|false}.",
    "rename": "{object, name}.",
    "delete": "Borrar objeto: {object}.",
    "set_setting": "Ajuste por objeto o pieza: {object, part?, key, value} (cualquier clave de proceso).",
    "remove_setting": "{object, part?, key}.",
    "set_extruder": "Filamento de objeto o pieza: {object, part?, extruder}.",
    "set_project_setting": "Ajuste del proyecto: {key, value}.",
    "paint": "Pintar: {object, part?, layer color|supports|seam|fuzzy_skin, frame world|local, max_edge?, "
             "shapes:[{shape box{min,max} | sphere{center,radius} | capsule{a,b,radius} | cylinder{center,radius,"
             "z_min?,z_max?} | height{z_min,z_max} | half_space{point,normal} | overhang{angle} | "
             "smart_fill{seed,angle} | all, extruder (color), mode enforce|block (supports/seam), normal?+min_dot?, "
             "erase?}]}. Coordenadas de la cama.",
    "paint_dots": "Topos: {object, part?, layer?, extruder|mode, radius, spacing, seed?}.",
    "clear_paint": "{object, part?, layer color|supports|seam|fuzzy_skin|all}.",
    "add_filament": "{copy_from, color, flush_volume?}.",
    "set_filament_color": "{filament, color}.",
    "set_flush_volumes": "{value} o {from, to, value}.",
    "add_part": "Añadir pieza/modificador: {object, type part|modifier|negative|support_enforcer|support_blocker, "
                "shape box|cylinder|disc|cone|sphere|torus + size/radius/height… o file (STL/OBJ/3MF/STEP), center "
                "[x,y,z] cama, rotate {x,y,z}, name?, extruder?, settings {clave: valor} (p. ej. modificador con "
                "sparse_infill_density)}.",
    "remove_part": "{object, part}.",
    "set_part_type": "{object, part, type}.",
    "rename_part": "{object, part, name}.",
    "move_part": "{object, part, x, y, z}.",
    "rotate_part": "{object, part, axis, degrees}.",
    "scale_part": "{object, part, factor}.",
    "add_object": "Añadir objeto: {shape … o file (STL/OBJ/3MF/STEP), position [x,y]?, rotate?, name?, extruder?, "
                  "scale?, settings?}. Sin position busca un hueco libre.",
    "add_instance": "Copias: {object, count, offset [dx,dy]?}.",
    "remove_instance": "{object, instance}.",
    "cut": "Cortar: {object, axis x|y|z + at (mm de la cama) o plane {point, normal}, keep both|upper|lower, "
           "place_on_cut? (true)}. Tapa las caras y deja cada mitad como objeto apoyado en la cama.",
    "split_to_parts": "{object, part?}: separa las islas de la malla en piezas.",
    "split_to_objects": "{object}: cada pieza sólida (o isla) pasa a ser un objeto.",
    "set_layer_range": "Rango de altura: {object, min_z, max_z, settings {layer_height, wall_loops, …}}.",
    "clear_layer_ranges": "{object}.",
    "set_variable_layer_height": "{object, ranges [{min_z, max_z, layer_height}] o points [[z, h], …], base?}.",
    "clear_variable_layer_height": "{object}.",
    "add_layer_gcode": "{plate?, z o layer, type pause|color_change|custom|tool_change|template, extruder?, color?, "
                       "message?, gcode?}.",
    "clear_layer_gcode": "{plate?, z?}.",
    "set_brim_ears": "Orejas de borde: {object, points [[x,y], …] o automáticas en esquinas, radius, max_angle?}.",
    "set_plate": "{plate, name?, bed_type?, print_sequence?, spiral_mode?, locked?}.",
    "set_project_info": "{title?, designer?, description?, license?, copyright?, origin?}.",
}


@bridge_method("model.operations", kind="any", doc="Catálogo de operaciones de edit_model con sus parámetros.")
def _model_operations(_params):
    extra = {k: (getattr(v, "__doc__", "") or "").strip() for k, v in EXTRA_3MF_OPS.items()}
    return {"operations": OPERATIONS_DOC, "module_operations": extra, "cli_actions": sorted(_CLI_FLAGS)}


@bridge_method("model.analyze", kind="worker",
               doc="Analiza el proyecto: tamaño, volumen, superficie, voladizos, contacto con la cama y pintura "
                   "por pieza; además rangos de altura, capa variable, G-code por capa, orejas y placas.")
async def _model_analyze(params):
    work = _work_dir("analyze")
    src = params.get("path") or await _export_current(work)
    p = Project3MF(src)
    return {"objects": p.analyze(float(params.get("overhang_angle", 45))), "layers": p.layer_info(),
            "filaments": (p.project or {}).get("filament_colour")}


@bridge_method("paint.inspect", kind="worker",
               doc="Informe de OrcaSlicer (--inspect-paint) de todo lo pintado: color, soportes, costura, piel difusa.")
async def _paint_inspect(params):
    work = _work_dir("inspect")
    src = params.get("path") or await _export_current(work)
    res = await run_process([_exe(), "--inspect-paint", src], work, timeout=300)
    text = res["stdout"].strip()
    start = min([i for i in (text.find("{"), text.find("[")) if i >= 0] or [0])
    try:
        return {"report": json.loads(text[start:])}
    except ValueError:
        return {"raw": text[-4000:], "stderr": res["stderr"][-1000:]}


_EXPORT_FORMATS = {"gcode", "3mf", "stl", "svg"}


@bridge_method("project.export", kind="worker", write=True,
               doc="Exporta el proyecto abierto con la CLI de OrcaSlicer a output_dir: gcode (lamina la placa 'plate', "
                   "0 = todas), 3mf, stl (un STL por objeto en output_dir/stl) o svg (capas).")
async def _project_export(params):
    fmt = str(params.get("format", "gcode")).lower()
    if fmt not in _EXPORT_FORMATS:
        raise BridgeError(f"format debe ser uno de {sorted(_EXPORT_FORMATS)}")
    out_dir = params.get("output_dir")
    if not out_dir:
        raise BridgeError("Indica output_dir: carpeta de destino.")
    work = _work_dir("export")
    src = await _export_current(work)
    base = params.get("name") or os.path.splitext(os.path.basename(src))[0]
    args = [_exe(), "--outputdir", out_dir]
    if fmt == "gcode":
        args += ["--slice", str(int(params.get("plate", 0)))]
    elif fmt == "3mf":
        args += ["--export-3mf", base + ".3mf"]
    elif fmt == "stl":
        args += ["--export-stl"]
    elif fmt == "svg":
        args += ["--export-svg"]
    def listing():
        out = {}
        for root, _d, fs in os.walk(out_dir):
            for f in fs:
                full = os.path.join(root, f)
                out[os.path.relpath(full, out_dir)] = os.path.getmtime(full)
        return out
    before = listing() if os.path.isdir(out_dir) else {}
    res = await run_process(args + [src], work, timeout=float(STATE.cfg.get("cli_timeout_s", 900)))
    after = listing() if os.path.isdir(out_dir) else {}
    files = sorted(f for f, t in after.items() if before.get(f) != t and f != "result.json")
    if res["returncode"] != 0 and not files:
        raise BridgeError(f"La CLI de OrcaSlicer falló (código {res['returncode']}): "
                          f"{(res['stderr'] or res['stdout'])[-800:]}")
    return {"format": fmt, "output_dir": out_dir, "files": files, "cli_seconds": res["seconds"]}


@bridge_method("project.open", kind="worker", write=True,
               doc="Abre un archivo (3MF, STL, STEP…) en la ventana de OrcaSlicer ya abierta.")
async def _project_open(params):
    path = params.get("path")
    if not path or not os.path.exists(path):
        raise BridgeError(f"El archivo no existe: {path!r}")
    return await _reopen(os.path.abspath(path))


def _parse_slice_outputs(work: str, result_3mf: str | None) -> dict:
    stats = {}
    for root, _dirs, files in os.walk(work):
        for f in files:
            if f.endswith(".statistics.json"):
                try:
                    with open(os.path.join(root, f), encoding="utf-8") as fh:
                        stats[f.replace(".statistics.json", "")] = json.load(fh)
                except ValueError:
                    pass
    plates = []
    if result_3mf and os.path.exists(result_3mf):
        with zipfile.ZipFile(result_3mf) as z:
            if "Metadata/slice_info.config" in z.namelist():
                root = ET.fromstring(z.read("Metadata/slice_info.config"))
                for plate in root.findall("plate"):
                    entry = {m.get("key"): m.get("value") for m in plate.findall("metadata")}
                    entry["filaments"] = [dict(f.attrib) for f in plate.findall("filament")]
                    entry["objects"] = [dict(o.attrib) for o in plate.findall("object")]
                    entry["warnings"] = [dict(w.attrib) for w in plate.findall("warning")]
                    plates.append(entry)
            gcodes = [n for n in z.namelist() if n.endswith(".gcode")]
    else:
        gcodes = []
    return {"statistics": stats, "plates": plates, "gcode_entries": gcodes}


@bridge_method("slice.headless", kind="worker",
               doc="Lamina en segundo plano una copia del proyecto actual (opcionalmente con cambios aplicados "
                   "solo a la copia) y devuelve tiempo, filamento y avisos por placa. No toca la ventana.")
async def _slice_headless(params):
    plate = int(params.get("plate", 0))
    async with STATE.write_lock:
        work = _work_dir("slice")
        src = await _export_current(work)
        ops = params.get("operations") or []
        overrides = params.get("project_settings") or {}
        if ops or overrides:
            p = Project3MF(src)
            for op in ops:
                p.apply(dict(op))
            for k, v in overrides.items():
                p.apply({"op": "set_project_setting", "key": k, "value": v})
            p.save(src)
        out_name = "laminado.3mf"
        args = [_exe(), "--slice", str(plate), "--export-statistics", "--outputdir", work,
                "--export-3mf", out_name, src]
        res = await run_process(args, work, timeout=float(STATE.cfg.get("cli_timeout_s", 900)))
        parsed = _parse_slice_outputs(work, os.path.join(work, out_name))
        if res["returncode"] != 0 and not parsed["plates"]:
            raise BridgeError(f"El laminado falló (código {res['returncode']}): "
                              f"{(res['stderr'] or res['stdout'])[-1200:]}")
        parsed.update({"returncode": res["returncode"], "cli_seconds": res["seconds"],
                       "sliced_project": os.path.join(work, out_name),
                       "log_tail": (res["stdout"] + res["stderr"])[-1500:] if res["returncode"] else ""})
        _cleanup_work()
        return parsed


@bridge_method("mesh.inspect", kind="worker",
               doc="Informe de mallas de OrcaSlicer (cajas y caras de la envolvente convexa sobre las que se puede apoyar).")
async def _mesh_inspect(_params):
    work = _work_dir("inspect")
    src = await _export_current(work)
    res = await run_process([_exe(), "--inspect-mesh", src], work, timeout=300)
    text = res["stdout"].strip()
    start = min([i for i in (text.find("{"), text.find("[")) if i >= 0] or [0])
    try:
        return {"report": json.loads(text[start:])}
    except ValueError:
        raise BridgeError(f"No pude interpretar la salida de --inspect-mesh: {text[-600:]}") from None


# ─────────────────────────────────────────────────────────────────────────────
# Métodos sin acceso a orca.host
# ─────────────────────────────────────────────────────────────────────────────

@bridge_method("bridge.reload", kind="worker", write=True,
               doc="Recarga los módulos de <carpeta del plugin>/modules (métodos y operaciones nuevas) sin reiniciar.")
def _bridge_reload(_params):
    return load_modules()


@bridge_method("bridge.methods", kind="any", doc="Métodos disponibles en el puente.")
def _bridge_methods(_params):
    return {n: {"kind": e["kind"], "write": e["write"], "doc": e["doc"]} for n, e in sorted(_METHODS.items())}


@bridge_method("bridge.status", kind="any", doc="Estado del puente.")
def _bridge_status(_params):
    return {"version": BRIDGE_VERSION, "port": STATE.port, "ui_pump_alive": STATE.pump_alive(),
            "served": STATE.served, "last_method": STATE.last_method, "last_error": STATE.last_error[-500:],
            "pending_ui_jobs": STATE.ui_jobs.qsize(), "uptime_s": round(time.time() - STATE.started_at, 1),
            "orca_executable": STATE.exe,
            "preset_write_api": hasattr(getattr(getattr(orca.host, "preset", None), "process", None), "begin_update"),
            "live_edit_api": live_edit_api() is not None,
            "ui_access": "directo (orca.host.edit.call_on_ui)" if _direct_ui_call() is not None else "panel"}


@bridge_method("events.get", kind="any", doc="Eventos de OrcaSlicer desde un número de secuencia.")
def _events_get(params):
    since, limit = int(params.get("since", 0)), int(params.get("limit", 100))
    items = [e for e in list(STATE.events) if e["seq"] > since][-limit:]
    return {"last_seq": STATE.event_seq, "events": items}


@bridge_method("slice.last", kind="any", doc="Resumen del último G-code exportado desde la ventana.")
def _slice_last(_params):
    return STATE.last_slice or {"available": False,
                                "hint": "Activa 'MCP G-code Analyzer' en el proceso (Otros → Plugin de "
                                        "postprocesado) o usa slice.headless."}


# ─────────────────────────────────────────────────────────────────────────────
# Despacho y servidor HTTP (asyncio, en el hilo del script; sin hilos propios)
# ─────────────────────────────────────────────────────────────────────────────

async def dispatch(method: str, params: dict) -> dict:
    entry = _METHODS.get(method)
    if entry is None:
        return {"ok": False, "error": f"Método desconocido: {method}"}
    try:
        if entry["kind"] == "ui":
            result = await on_ui(entry["fn"], params)
        else:
            result = entry["fn"](params)
            if inspect.isawaitable(result):
                result = await result
        STATE.served += 1
        STATE.last_method = method
        return {"ok": True, "result": _jsonable(result)}
    except BridgeError as exc:
        STATE.last_error = str(exc)
        return {"ok": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        STATE.last_error = traceback.format_exc()
        traceback.print_exc()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


async def _respond(writer, code: int, payload: dict):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 403: "Forbidden", 404: "Not Found",
              413: "Payload Too Large"}.get(code, "OK")
    writer.write(f"HTTP/1.1 {code} {reason}\r\nContent-Type: application/json; charset=utf-8\r\n"
                 f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("latin-1") + body)
    await writer.drain()


async def _handle_client(reader, writer):
    try:
        line = await asyncio.wait_for(reader.readline(), 30)
        parts = line.decode("latin-1").split()
        if len(parts) < 2:
            return
        verb, path = parts[0], parts[1]
        headers = {}
        while True:
            h = await asyncio.wait_for(reader.readline(), 30)
            if h in (b"\r\n", b"\n", b""):
                break
            k, _, v = h.decode("latin-1").partition(":")
            headers[k.strip().lower()] = v.strip()
        if "origin" in headers:  # navegadores: nunca
            return await _respond(writer, 403, {"ok": False, "error": "forbidden"})
        if verb == "GET" and path == "/health":
            return await _respond(writer, 200, {"ok": True, "service": "orca-mcp-bridge", "version": BRIDGE_VERSION})
        if verb != "POST" or path != "/rpc":
            return await _respond(writer, 404, {"ok": False, "error": "not found"})
        if not secrets.compare_digest(headers.get("authorization", ""), f"Bearer {STATE.token}"):
            return await _respond(writer, 401, {"ok": False, "error": "unauthorized"})
        n = int(headers.get("content-length", "0") or 0)
        if n <= 0 or n > MAX_BODY_BYTES:
            return await _respond(writer, 413 if n > 0 else 400, {"ok": False, "error": "bad body"})
        try:
            req = json.loads(await reader.readexactly(n))
            method, params = str(req["method"]), req.get("params") or {}
        except Exception as exc:  # noqa: BLE001
            return await _respond(writer, 400, {"ok": False, "error": f"bad request: {exc}"})
        await _respond(writer, 200, await dispatch(method, params))
    except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


def _listening_socket(port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if not sys.platform.startswith("win"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((LISTEN_ADDR, port))
    except OSError:
        sock.bind((LISTEN_ADDR, 0))
    sock.listen(16)
    sock.setblocking(False)
    return sock


def _write_bridge_file():
    info = {"url": f"http://{LISTEN_ADDR}:{STATE.port}", "port": STATE.port, "token": STATE.token,
            "version": BRIDGE_VERSION, "pid": os.getpid(), "started_at": STATE.started_at}
    tmp = STATE.bridge_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(info, fh)
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, STATE.bridge_file)


# ─────────────────────────────────────────────────────────────────────────────
# Panel acoplado: bomba del hilo UI + estado
# ─────────────────────────────────────────────────────────────────────────────

PANEL_HTML = r"""<!doctype html><html><head><meta charset="utf-8">
<style>
 body{font-family:var(--orca-font);margin:10px;color:var(--orca-fg);background:var(--orca-bg);font-size:12px}
 h3{margin:0 0 8px;font-size:14px;display:flex;align-items:center;gap:6px}
 .row{display:flex;justify-content:space-between;gap:8px;padding:4px 0;border-bottom:1px solid var(--orca-border)}
 .k{color:var(--orca-muted)} .dot{width:8px;height:8px;border-radius:50%;background:gray;display:inline-block}
 #help{margin-top:8px;color:var(--orca-muted);line-height:1.4}
 button{margin-top:8px;padding:4px 10px;background:var(--orca-bg-2);color:var(--orca-fg);border:1px solid var(--orca-border);border-radius:4px;cursor:pointer}
</style></head><body>
<h3><span id="dot" class="dot"></span>MCP Bridge</h3>
<div class="row"><span class="k">Servidor</span><span id="url">detenido</span></div>
<div class="row"><span class="k">Peticiones</span><span id="served">0</span></div>
<div class="row"><span class="k">Último método</span><span id="last">—</span></div>
<div class="row"><span class="k">Último error</span><span id="err" title="">—</span></div>
<div class="row"><span class="k">Canal</span><span id="chan">—</span></div>
<div id="help">Para activarlo: <b>Plugins</b> → MCP Bridge → <b>MCP Bridge: servidor</b> → Ejecutar.
Deja este panel abierto: el puente lo usa para leer el modelo.</div>
<button id="stop" style="display:none">Detener servidor</button>
<script>
 const $=id=>document.getElementById(id);
 // Canal hacia Python. OrcaSlicer inyecta window.orca (que usa window.wx); si la página se cargó antes
 // de que existiera el manejador "wx", se usa el canal nativo de WebView2 con el mismo formato.
 function rawSend(d){
   const msg=JSON.stringify({channel:"orca",kind:"message",data:d});
   try{ if(window.wx&&typeof window.wx.postMessage==="function"){window.wx.postMessage(msg);return "wx";} }catch(e){}
   try{ if(window.chrome&&window.chrome.webview){window.chrome.webview.postMessage(msg);return "webview2";} }catch(e){}
   try{ if(window.webkit&&window.webkit.messageHandlers&&window.webkit.messageHandlers.wx){window.webkit.messageHandlers.wx.postMessage(msg);return "webkit";} }catch(e){}
   return "ninguno";
 }
 function onStatus(d){
   if(!d||d.type!=="status")return;
   $("url").textContent=d.running?d.url:"detenido";
   $("served").textContent=d.served; $("last").textContent=d.last_method||"—";
   $("err").textContent=d.last_error?"sí (pasa el ratón)":"—"; $("err").title=d.last_error||"";
   $("dot").style.background=d.running?"var(--orca-accent, #00ae42)":"gray";
   $("help").style.display=d.running?"none":"block"; $("stop").style.display=d.running?"inline-block":"none";
 }
 // Respuestas de Python: OrcaSlicer llama a window.__orcaDispatch; si su script no se inyectó, lo definimos.
 if(window.orca&&typeof window.orca.onMessage==="function"){ window.orca.onMessage(onStatus); }
 else { window.__orcaDispatch=function(p){ onStatus(p?p.data:null); }; }
 $("stop").onclick=()=>rawSend({type:"stop"});
 let chan=rawSend({type:"hello"}); $("chan").textContent=chan;
 if(chan==="ninguno"&&!sessionStorage.getItem("mcpReloaded")){
   try{sessionStorage.setItem("mcpReloaded","1");}catch(e){}
   setTimeout(()=>location.reload(),1500);
 }
 setInterval(()=>{ chan=rawSend({type:"tick"}); $("chan").textContent=chan; },120);
</script></body></html>"""


def _panel_status():
    return {"type": "status", "running": STATE.running, "url": f"http://{LISTEN_ADDR}:{STATE.port}",
            "served": STATE.served, "last_method": STATE.last_method, "last_error": STATE.last_error[-300:]}


class _PanelHost:
    _last = 0.0

    @classmethod
    def on_message(cls, data):
        kind = (data or {}).get("type")
        STATE.pump()
        if kind == "stop" and STATE.loop is not None and STATE.stop_event is not None:
            STATE.loop.call_soon_threadsafe(STATE.stop_event.set)
        now = time.time()
        if kind in ("hello", "stop") or now - cls._last > 1.0:
            cls._last = now
            if STATE.panel is not None:
                try:
                    STATE.panel.post(_panel_status())
                except Exception:  # noqa: BLE001
                    pass

    @classmethod
    def on_close(cls):
        STATE.panel = None


def open_panel():
    if STATE.panel is not None:
        try:
            if STATE.panel.is_open():
                STATE.panel.show()
                return
        except Exception:  # noqa: BLE001
            pass
    STATE.panel = orca.host.ui.create_dock_panel(
        html=PANEL_HTML, title="MCP Bridge", width=290, height=300,
        on_message=_PanelHost.on_message, on_close=_PanelHost.on_close, dock="right")


# ─────────────────────────────────────────────────────────────────────────────
# Capacidad 1: el servidor (script de larga duración)
# ─────────────────────────────────────────────────────────────────────────────

# Con orca.host.edit.call_on_ui (MCPOrcaSlicer) el panel ya no hace falta para bombear llamadas al hilo de la interfaz.
DEFAULT_CONFIG = {"port": DEFAULT_PORT, "show_panel": live_edit_api() is None or not hasattr(live_edit_api(), "call_on_ui"),
                  "orca_executable": "", "cli_timeout_s": 900}


class BridgeServer(orca.script.ScriptPluginCapabilityBase):
    def get_name(self):
        return "MCP Bridge: servidor"

    def get_default_config(self):
        return dict(DEFAULT_CONFIG)

    def _cfg(self):
        try:
            cfg = json.loads(self.get_config() or "{}")
        except Exception:  # noqa: BLE001
            cfg = {}
        out = dict(DEFAULT_CONFIG)
        out.update(cfg if isinstance(cfg, dict) else {})
        return out

    def on_load(self):
        try:
            STATE.storage = str(orca.host.plugin.storage())
            if self._cfg().get("show_panel", True):
                open_panel()
        except Exception:  # noqa: BLE001
            traceback.print_exc()

    def on_unload(self):
        if STATE.loop is not None and STATE.stop_event is not None:
            try:
                STATE.loop.call_soon_threadsafe(STATE.stop_event.set)
            except RuntimeError:
                pass

    async def execute(self):
        if STATE.running:
            return orca.ExecutionResult.skipped(f"MCP Bridge ya está activo en http://{LISTEN_ADDR}:{STATE.port}")
        STATE.cfg = self._cfg()
        STATE.storage = str(orca.host.plugin.storage())
        STATE.bridge_file = os.path.join(STATE.storage, BRIDGE_FILE_NAME)
        STATE.exe = find_orca_executable(STATE.cfg.get("orca_executable", ""))
        STATE.token = secrets.token_urlsafe(32)
        STATE.started_at = time.time()
        STATE.loop = asyncio.get_running_loop()
        STATE.stop_event = asyncio.Event()
        STATE.write_lock = asyncio.Lock()
        server = None
        try:
            sock = _listening_socket(int(STATE.cfg.get("port", DEFAULT_PORT)))
            STATE.port = sock.getsockname()[1]
            server = await asyncio.start_server(_handle_client, sock=sock)
            try:
                load_modules()
            except Exception:  # noqa: BLE001
                traceback.print_exc()
            _write_bridge_file()
            if STATE.cfg.get("show_panel", True):
                open_panel()
            print(f"[MCP Bridge] escuchando en http://{LISTEN_ADDR}:{STATE.port}")
            await STATE.stop_event.wait()
        finally:
            if server is not None:
                server.close()
                try:
                    await asyncio.wait_for(server.wait_closed(), 3)
                except (asyncio.TimeoutError, Exception):  # noqa: BLE001
                    pass
            try:
                os.remove(STATE.bridge_file)
            except OSError:
                pass
            STATE.loop = None
            STATE.stop_event = None
            STATE.port = 0
        return orca.ExecutionResult.success("MCP Bridge detenido")

    def on_lifecycle_event(self, event, ctx):
        rec = {"event": _jsonable(event)}
        for f in ("code", "name", "msg", "id", "previous_name", "device_id", "job_id", "source", "index", "dirty"):
            v = getattr(ctx, f, None)
            if v not in (None, "", -1):
                rec[f] = _jsonable(v)
        STATE.add_event(rec)


# ─────────────────────────────────────────────────────────────────────────────
# Capacidad 2: analizador de G-code (no modifica el archivo)
# ─────────────────────────────────────────────────────────────────────────────

_STAT_RE = re.compile(r"^;\s*(?P<k>[^=:]*?(estimated|filament used|total|layer|time|cost|weight)[^=:]*?)\s*[=:]\s*(?P<v>.+)$",
                      re.IGNORECASE)
_SUMMARY_KEYS = ("layer_height", "initial_layer_print_height", "wall_loops", "sparse_infill_density",
                 "sparse_infill_pattern", "enable_support", "nozzle_temperature", "hot_plate_temp",
                 "outer_wall_speed", "inner_wall_speed", "nozzle_diameter")


def analyze_gcode(path: str, max_stats: int = 60) -> dict:
    stats, layers, max_z, lines, moves = {}, 0, 0.0, 0, 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            lines += 1
            if line.startswith(";"):
                s = line.strip()
                if s.startswith(";LAYER_CHANGE"):
                    layers += 1
                elif s.startswith(";Z:"):
                    try:
                        max_z = max(max_z, float(s[3:]))
                    except ValueError:
                        pass
                elif len(stats) < max_stats:
                    m = _STAT_RE.match(s)
                    if m:
                        stats.setdefault(m.group("k").strip(), m.group("v").strip())
            elif line[:3] in ("G0 ", "G1 "):
                moves += 1
    return {"lines": lines, "moves": moves, "layer_changes": layers, "max_z_mm": max_z,
            "size_bytes": os.path.getsize(path), "stats": stats}


class GCodeAnalyzer(orca.slicing.SlicingPipelineCapabilityBase):
    def get_name(self):
        return "MCP G-code Analyzer"

    def execute(self, ctx):
        if ctx.step != orca.slicing.Step.psGCodePostProcess or not ctx.gcode_path:
            return orca.ExecutionResult.success()
        try:
            s = analyze_gcode(ctx.gcode_path)
            s.update({"available": True, "analyzed_at": time.time(), "gcode_path": ctx.gcode_path,
                      "output_name": ctx.output_name,
                      "config": {k: _jsonable(ctx.config_value(k)) for k in _SUMMARY_KEYS}})
            STATE.last_slice = s
        except Exception as exc:  # noqa: BLE001
            STATE.last_error = f"analyzer: {exc}"
            traceback.print_exc()
        return orca.ExecutionResult.success("G-code analizado")


# ─────────────────────────────────────────────────────────────────────────────
# Paquete
# ─────────────────────────────────────────────────────────────────────────────

@orca.plugin
class MCPBridgePlugin(orca.base):
    def register_capabilities(self):
        # Sockets solo en localhost y lanzar el propio OrcaSlicer (CLI y reapertura de proyectos).
        exe = find_orca_executable()
        orca.request_permissions(network_socket=[LISTEN_ADDR], process=[exe] if exe else [])
        orca.register_capability(BridgeServer)
        orca.register_capability(GCodeAnalyzer)
