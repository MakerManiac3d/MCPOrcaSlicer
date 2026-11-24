#!/usr/bin/env python3
"""Cambio de paleta de MCPOrcaSlicer: el verde de marca de OrcaSlicer pasa a rojo.

Se aplica sobre el árbol de trabajo antes de compilar (lo hace .github/workflows/mcp_local_build.yml), para que el
fork no tenga que mantener cambios en los ~600 archivos donde el verde está escrito a mano y las actualizaciones
desde OrcaSlicer no choquen. Es idempotente: ejecutarlo dos veces no cambia nada más.

Solo se sustituyen los colores exactos de la paleta de marca (no cualquier verde: las paletas de los selectores
de color de las páginas web se quedan como están), en:
  - src/slic3r/GUI         (código de la interfaz: literales "#009688", rgb(0, 150, 136), wxColour(0,150,136))
  - resources/images       (iconos SVG)
  - resources/web          (HTML/CSS/JS de los diálogos internos)
y la franja verde del logotipo e icono del programa (OrcaSlicer*.png/.ico) se recolorea a rojo.

Uso:  python scripts/mcp_rebrand.py [--check]     (--check: solo cuenta lo que cambiaría)
"""
import argparse
import colorsys
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# verde de OrcaSlicer -> rojo de MCPOrcaSlicer
PALETTE = {
    "009688": "C62828",  # color principal
    "009789": "C72929",
    "009687": "C62828",
    "26A69A": "E53935",  # resalte (hover)
    "008172": "B71C1C",  # resalte oscuro
    "00675B": "8E1C1C",  # principal en modo oscuro
    "33ABA1": "D84343",
    "BFE1DE": "F6D5D5",  # tono claro
    "E5F0EE": "FBEDED",  # fondo claro
}
RGB_PALETTE = {tuple(int(k[i:i + 2], 16) for i in (0, 2, 4)): tuple(int(v[i:i + 2], 16) for i in (0, 2, 4))
               for k, v in PALETTE.items()}

TEXT_DIRS = [("src/slic3r/GUI", (".cpp", ".hpp", ".h")),
             ("src/libslic3r/Color.hpp", (".hpp",)),
             ("resources/images", (".svg",)),
             ("resources/web", (".html", ".htm", ".css", ".js", ".svg", ".json"))]
LOGO_PREFIX = "OrcaSlicer"
LOGO_EXT = (".png", ".ico")

HEX_RE = re.compile(r"#(%s)\b" % "|".join(PALETTE), re.IGNORECASE)
# Tres componentes de color tras "(" o "{": enteros 0-255, fracciones "150.f / 255.0f" (o "0.f") y decimales 0-1.
_INT = r"\d{1,3}"
_RATIO = r"(?:\d+(?:\.\d*)?f?\s*/\s*255(?:\.\d*)?f?|0(?:\.0*)?f?)"
_DEC = r"(?:0|1)?\.\d+f?|0f?|1f?"
TRIPLE_RE = re.compile(r"([({]\s*)(%s)(\s*,\s*)(%s)(\s*,\s*)(%s)(?=\s*[,)}])" % (_INT, _INT, _INT))
RATIO_RE = re.compile(r"([({]\s*)(%s)(\s*,\s*)(%s)(\s*,\s*)(%s)(?=\s*[,)}])" % (_RATIO, _RATIO, _RATIO))
DEC_RE = re.compile(r"(ImVec4\(\s*)(%s)(\s*,\s*)(%s)(\s*,\s*)(%s)(?=\s*[,)])" % (_DEC, _DEC, _DEC))


def _ratio_value(tok):
    t = tok.replace(" ", "")
    if "/" in t:
        return float(t.split("/")[0].rstrip("f")) / 255.0
    return float(t.rstrip("f") or 0)


def _palette_near(rgb01, tol):
    for src, dst in RGB_PALETTE.items():
        if all(abs(a - b / 255.0) <= tol for a, b in zip(rgb01, src)):
            return dst
    return None


def recolor_text(text: str) -> tuple[str, int]:
    n = 0

    def hex_sub(m):
        nonlocal n
        new = PALETTE[m.group(1).upper()]
        n += 1
        return "#" + (new if m.group(1).isupper() or m.group(1).isdigit() else new.lower())

    def triple_sub(m):
        nonlocal n
        rgb = (int(m.group(2)), int(m.group(4)), int(m.group(6)))
        if rgb not in RGB_PALETTE:
            return m.group(0)
        n += 1
        r, g, b = RGB_PALETTE[rgb]
        return f"{m.group(1)}{r}{m.group(3)}{g}{m.group(5)}{b}"
    def ratio_sub(m):
        nonlocal n
        toks = (m.group(2), m.group(4), m.group(6))
        if not any("/" in t for t in toks):
            return m.group(0)
        dst = _palette_near([_ratio_value(t) for t in toks], 0.5 / 255)
        if dst is None:
            return m.group(0)
        n += 1
        r, g, b = dst
        return f"{m.group(1)}{r} / 255.f{m.group(3)}{g} / 255.f{m.group(5)}{b} / 255.f"

    def dec_sub(m):
        nonlocal n
        toks = (m.group(2), m.group(4), m.group(6))
        dst = _palette_near([float(t.rstrip("f") or 0) for t in toks], 0.006)
        if dst is None:
            return m.group(0)
        n += 1
        r, g, b = (f"{v / 255:.2f}f" for v in dst)
        return f"{m.group(1)}{r}{m.group(3)}{g}{m.group(5)}{b}"
    text = HEX_RE.sub(hex_sub, text)
    text = TRIPLE_RE.sub(triple_sub, text)
    text = RATIO_RE.sub(ratio_sub, text)
    text = DEC_RE.sub(dec_sub, text)
    return text, n


def recolor_pixel(r, g, b):
    """Píxeles turquesa (y sus bordes suavizados) -> rojo, conservando su luminosidad relativa."""
    h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
    if not (0.42 <= h <= 0.53 and s > 0.25 and 0.08 < l < 0.95):
        return None
    # #009688 (h 174°, s 1.0, l 0.29) -> #C62828 (h 0°, s 0.66, l 0.47)
    nl = min(0.95, l * 0.47 / 0.29) if l <= 0.29 else 0.47 + (l - 0.29) * (0.95 - 0.47) / (0.95 - 0.29)
    nr, ng, nb = colorsys.hls_to_rgb(0.0, nl, s * 0.66)
    return int(round(nr * 255)), int(round(ng * 255)), int(round(nb * 255))


def recolor_image(path: str, check: bool) -> int:
    from PIL import Image
    im = Image.open(path)
    frames = []
    changed = 0
    sizes = getattr(im, "ico", None) and im.ico.sizes() or None
    imgs = [im.ico.getimage(s).convert("RGBA") for s in sorted(sizes)] if sizes else [im.convert("RGBA")]
    for img in imgs:
        px = img.load()
        for y in range(img.height):
            for x in range(img.width):
                r, g, b, a = px[x, y]
                if a == 0:
                    continue
                new = recolor_pixel(r, g, b)
                if new is not None and new != (r, g, b):
                    px[x, y] = (*new, a)
                    changed += 1
        frames.append(img)
    if changed and not check:
        if sizes:
            frames[-1].save(path, format="ICO", sizes=[f.size for f in frames], append_images=frames[:-1])
        else:
            frames[0].save(path)
    return changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    total_files = total_hits = 0
    for rel, exts in TEXT_DIRS:
        base = os.path.join(ROOT, rel)
        walk = [(os.path.dirname(base), [], [os.path.basename(base)])] if os.path.isfile(base) else os.walk(base)
        for dp, _, fs in walk:
            for f in fs:
                if not f.lower().endswith(exts):
                    continue
                p = os.path.join(dp, f)
                with open(p, "rb") as fh:
                    raw = fh.read()
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    continue
                new, n = recolor_text(text)
                if n:
                    total_files += 1
                    total_hits += n
                    if not args.check:
                        with open(p, "wb") as fh:
                            fh.write(new.encode("utf-8"))
    logos = 0
    img_dir = os.path.join(ROOT, "resources", "images")
    try:
        import PIL  # noqa: F401
        for f in sorted(os.listdir(img_dir)):
            if f.startswith(LOGO_PREFIX) and f.lower().endswith(LOGO_EXT) and "grayscale" not in f:
                if recolor_image(os.path.join(img_dir, f), args.check):
                    logos += 1
    except ImportError:
        print("Aviso: sin Pillow no se recolorean los logotipos PNG/ICO (pip install pillow).", file=sys.stderr)
    print(f"Paleta MCPOrcaSlicer: {total_hits} colores en {total_files} archivos, {logos} logotipos"
          + (" (comprobación, sin cambios)" if args.check else ""))


if __name__ == "__main__":
    main()
