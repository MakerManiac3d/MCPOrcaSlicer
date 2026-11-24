#include "PluginHostBindings.hpp"
#include "PluginHostUi.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <libslic3r/BrimEarsPoint.hpp>
#include <libslic3r/CustomGCode.hpp>
#include <libslic3r/Geometry.hpp>
#include <libslic3r/CutUtils.hpp>
#include <libslic3r/Format/ProjectStrategy.hpp>
#include <libslic3r/Model.hpp>
#include <libslic3r/PresetBundle.hpp>
#include <libslic3r/PrintConfig.hpp>
#include <libslic3r/Slicing.hpp>
#include <libslic3r/TriangleMesh.hpp>
#include <slic3r/GUI/GLCanvas3D.hpp>
#include <slic3r/GUI/GUI.hpp>
#include <slic3r/GUI/GUI_App.hpp>
#include <slic3r/GUI/GUI_ObjectList.hpp>
#include <slic3r/GUI/ObjectDataViewModel.hpp>
#include <slic3r/GUI/PartPlate.hpp>
#include <slic3r/GUI/Plater.hpp>

#include <boost/filesystem/path.hpp>
#include <wx/colour.h>
#include <wx/string.h>

#include <algorithm>
#include <cmath>
#include <iterator>
#include <memory>
#include <array>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace py = pybind11;

// Write access to the live project for plugins: the same operations the user performs by hand in the
// 3D view and the object list (transforms, per-object settings, parts, painting, cut, layer ranges,
// per-layer G-code), applied in place, each one recorded as a single undo step.
//
// Every function converts its Python arguments to C++ first and then runs the edit on the UI thread
// through PluginHostUi::run_on_ui_thread (the GIL is released meanwhile), so a plugin may call them
// from its script thread or from the UI thread. Edits go through the same Plater/ObjectList refresh
// paths the GUI uses, so the 3D view, the object list, the plates and the background slicing all
// follow. Indices are 0-based, as in orca.host.model().

namespace Slic3r {
namespace {

using Vec3Opt = std::optional<std::array<double, 3>>;

GUI::Plater& plater()
{
    GUI::Plater* p = GUI::wxGetApp().plater();
    if (p == nullptr)
        throw std::runtime_error("Plater is not available");
    return *p;
}

ModelObject& object_at(int obj_idx)
{
    ModelObjectPtrs& objects = plater().model().objects;
    if (obj_idx < 0 || obj_idx >= int(objects.size()))
        throw py::index_error("object index " + std::to_string(obj_idx) + " out of range");
    return *objects[obj_idx];
}

ModelVolume& volume_at(ModelObject& object, int vol_idx)
{
    if (vol_idx < 0 || vol_idx >= int(object.volumes.size()))
        throw py::index_error("volume index " + std::to_string(vol_idx) + " out of range");
    return *object.volumes[vol_idx];
}

ModelInstance& instance_at(ModelObject& object, int inst_idx)
{
    if (inst_idx < 0 || inst_idx >= int(object.instances.size()))
        throw py::index_error("instance index " + std::to_string(inst_idx) + " out of range");
    return *object.instances[inst_idx];
}

Vec3d to_vec(const std::array<double, 3>& a) { return Vec3d(a[0], a[1], a[2]); }

// The object list refresh the GUI uses after parts are added, removed or retyped; it also reloads the
// scene and reschedules slicing (ObjectList::changed_object -> Plater::changed_object).
void refresh_object_volumes(int obj_idx)
{
    GUI::ObjectList* list = GUI::wxGetApp().obj_list();
    ModelObject&     mo   = object_at(obj_idx);
    if (list != nullptr && mo.volumes.size() > 1) {
        list->reorder_volumes_and_get_selection(obj_idx);
    } else {
        if (list != nullptr)
            list->update_info_items(size_t(obj_idx));
        plater().changed_object(obj_idx);
    }
}

void drop_instance_to_bed(ModelObject& object, int inst_idx)
{
    ModelInstance& inst = instance_at(object, inst_idx);
    object.invalidate_bounding_box();
    const double min_z = object.instance_bounding_box(size_t(inst_idx)).min.z();
    Vec3d        off   = inst.get_offset();
    off.z() -= min_z;
    inst.set_offset(off);
}

// Plain C++ copies of the Python arguments, taken while the GIL is held.
std::vector<std::array<double, 3>> to_vertices(const py::object& seq)
{
    auto v = py::cast<std::vector<std::array<double, 3>>>(seq);
    if (v.empty())
        throw py::value_error("vertices is empty");
    return v;
}

std::vector<std::array<int, 3>> to_triangles(const py::object& seq, size_t n_vertices)
{
    auto t = py::cast<std::vector<std::array<int, 3>>>(seq);
    if (t.empty())
        throw py::value_error("triangles is empty");
    for (const auto& tri : t)
        for (int i : tri)
            if (i < 0 || size_t(i) >= n_vertices)
                throw py::value_error("triangle vertex index " + std::to_string(i) + " out of range");
    return t;
}

TriangleMesh make_mesh(const std::vector<std::array<double, 3>>& vertices, const std::vector<std::array<int, 3>>& triangles)
{
    indexed_triangle_set its;
    its.vertices.reserve(vertices.size());
    for (const auto& p : vertices)
        its.vertices.emplace_back(float(p[0]), float(p[1]), float(p[2]));
    its.indices.reserve(triangles.size());
    for (const auto& t : triangles)
        its.indices.emplace_back(t[0], t[1], t[2]);
    return TriangleMesh(std::move(its));
}

ModelVolumeType volume_type_from_string(const std::string& type)
{
    if (type == "part")             return ModelVolumeType::MODEL_PART;
    if (type == "negative")         return ModelVolumeType::NEGATIVE_VOLUME;
    if (type == "modifier")         return ModelVolumeType::PARAMETER_MODIFIER;
    if (type == "support_enforcer") return ModelVolumeType::SUPPORT_ENFORCER;
    if (type == "support_blocker")  return ModelVolumeType::SUPPORT_BLOCKER;
    throw py::value_error("unknown part type \"" + type + "\": part, negative, modifier, support_enforcer or support_blocker");
}

FacetsAnnotation& facets_for(ModelVolume& volume, const std::string& layer)
{
    if (layer == "color")      return volume.mmu_segmentation_facets;
    if (layer == "supports")   return volume.supported_facets;
    if (layer == "seam")       return volume.seam_facets;
    if (layer == "fuzzy_skin") return volume.fuzzy_skin_facets;
    throw py::value_error("unknown paint layer \"" + layer + "\": color, supports, seam or fuzzy_skin");
}

void check_paint_codes(const std::vector<std::string>& codes)
{
    for (const std::string& code : codes)
        for (char ch : code)
            if (!((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F')))
                throw py::value_error("paint codes are upper-case hex strings as stored in 3MF (\"4\", \"8\", \"0C\", ...)");
}

// Fill a painting layer from per-triangle TriangleSelector strings ("" = unpainted), the encoding of
// the 3MF paint_color/paint_supports/paint_seam/paint_fuzzy_skin attributes.
void write_paint(FacetsAnnotation& facets, const std::vector<std::string>& codes)
{
    facets.reset();
    facets.reserve(int(codes.size()));
    for (size_t i = 0; i < codes.size(); ++i)
        if (!codes[i].empty())
            facets.set_triangle_from_string(int(i), codes[i]);
    facets.shrink_to_fit();
    // set_triangle_from_string() does not touch the timestamp; re-assign so the change is seen by the
    // scene, the undo stack and the background slicing.
    TriangleSelector::TriangleSplittingData data = facets.get_data();
    facets.set_data(std::move(data));
}

void set_config_values(ModelConfig& config, const std::map<std::string, std::string>& values, const std::vector<std::string>& erase)
{
    // Validate everything first so a bad key or value leaves the config untouched.
    DynamicPrintConfig staged;
    ConfigSubstitutionContext ctx(ForwardCompatibilitySubstitutionRule::Disable);
    for (const auto& [key, value] : values) {
        if (print_config_def.get(key) == nullptr)
            throw py::value_error("unknown setting \"" + key + "\"");
        staged.set_deserialize(key, value, ctx);
    }
    for (const std::string& key : erase)
        config.erase(key);
    if (!values.empty())
        config.apply(staged);
}

// ---------------------------------------------------------------------------------------------------

void set_instance_transform(int obj_idx, int inst_idx, Vec3Opt offset, Vec3Opt rotation, Vec3Opt scale, Vec3Opt mirror,
                            bool drop_to_bed)
{
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject&   object = object_at(obj_idx);
        ModelInstance& inst   = instance_at(object, inst_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: transform object");
        if (rotation) inst.set_rotation(to_vec(*rotation));
        if (scale)    inst.set_scaling_factor(to_vec(*scale));
        if (mirror)   inst.set_mirror(to_vec(*mirror));
        if (offset)   inst.set_offset(to_vec(*offset));
        if (drop_to_bed)
            drop_instance_to_bed(object, inst_idx);
        object.invalidate_bounding_box();
        plater().get_partplate_list().notify_instance_update(obj_idx, inst_idx);
        plater().changed_object(obj_idx);
    });
}

void set_volume_transform(int obj_idx, int vol_idx, Vec3Opt offset, Vec3Opt rotation, Vec3Opt scale, Vec3Opt mirror)
{
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        ModelVolume& volume = volume_at(object, vol_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: transform part");
        if (rotation) volume.set_rotation(to_vec(*rotation));
        if (scale)    volume.set_scaling_factor(to_vec(*scale));
        if (mirror)   volume.set_mirror(to_vec(*mirror));
        if (offset)   volume.set_offset(to_vec(*offset));
        object.invalidate_bounding_box();
        plater().changed_object(obj_idx);
    });
}

Transform3d to_transform(const std::array<double, 16>& m)
{
    Transform3d t = Transform3d::Identity();
    for (int r = 0; r < 4; ++r)
        for (int c = 0; c < 4; ++c)
            t.matrix()(r, c) = m[r * 4 + c];
    if (std::abs(t.linear().determinant()) < 1e-12)
        throw py::value_error("the matrix is singular");
    return t;
}

// Whole 4x4 (row-major) placement, as stored in 3MF build items and components.
void set_instance_matrix(int obj_idx, int inst_idx, std::array<double, 16> matrix)
{
    const Transform3d t = to_transform(matrix);
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: transform object");
        instance_at(object, inst_idx).set_transformation(Geometry::Transformation(t));
        object.invalidate_bounding_box();
        plater().get_partplate_list().notify_instance_update(obj_idx, inst_idx);
        plater().changed_object(obj_idx);
    });
}

void set_volume_matrix(int obj_idx, int vol_idx, std::array<double, 16> matrix)
{
    const Transform3d t = to_transform(matrix);
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: transform part");
        volume_at(object, vol_idx).set_transformation(Geometry::Transformation(t));
        object.invalidate_bounding_box();
        plater().changed_object(obj_idx);
    });
}

void set_config(int obj_idx, int vol_idx, std::map<std::string, std::string> values, std::vector<std::string> erase)
{
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        ModelVolume* volume = vol_idx >= 0 ? &volume_at(object, vol_idx) : nullptr;
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: change settings");
        set_config_values(volume ? volume->config : object.config, values, erase);
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->object_config_options_changed(GUI::ObjectVolumeID{&object, volume});
        if (values.count("extruder") || std::count(erase.begin(), erase.end(), "extruder"))
            if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
                list->update_filament_values_for_items(GUI::wxGetApp().filaments_cnt());
        plater().changed_object(obj_idx);
    });
}

void rename(int obj_idx, int vol_idx, std::string name)
{
    if (name.empty())
        throw py::value_error("name is empty");
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: rename");
        if (vol_idx >= 0)
            volume_at(object, vol_idx).name = name;
        else
            object.name = name;
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->update_name_in_list(obj_idx, vol_idx);
    });
}

void set_printable(int obj_idx, int inst_idx, bool printable)
{
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: set printable");
        instance_at(object, inst_idx).printable = printable;
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->update_printable_state(obj_idx, inst_idx);
        plater().changed_object(obj_idx);
    });
}

void delete_object(int obj_idx)
{
    PluginHostUi::run_on_ui_thread([&] {
        object_at(obj_idx);
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->delete_from_model_and_list(GUI::itObject, obj_idx, -1);
        else
            throw std::runtime_error("The object list is not available");
    });
}

void delete_volume(int obj_idx, int vol_idx)
{
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        ModelVolume& volume = volume_at(object, vol_idx);
        int solid = 0;
        for (const ModelVolume* v : object.volumes)
            solid += v->is_model_part();
        if (volume.is_model_part() && solid == 1)
            throw std::invalid_argument("the last solid part of an object cannot be deleted; delete the object");
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->delete_from_model_and_list(GUI::itVolume, obj_idx, vol_idx);
    });
}

void delete_instance(int obj_idx, int inst_idx)
{
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        instance_at(object, inst_idx);
        if (object.instances.size() == 1)
            throw std::invalid_argument("the last instance of an object cannot be deleted; delete the object");
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->delete_from_model_and_list(GUI::itInstance, obj_idx, inst_idx);
    });
}

int add_instance(int obj_idx, std::array<double, 3> offset, Vec3Opt rotation, Vec3Opt scale)
{
    int new_idx = -1;
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        const ModelInstance& last = *object.instances.back();
        const bool was_one = object.instances.size() == 1;
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: add instance");
        object.add_instance(to_vec(offset), scale ? to_vec(*scale) : last.get_scaling_factor(),
                            rotation ? to_vec(*rotation) : last.get_rotation(), last.get_mirror());
        new_idx = int(object.instances.size()) - 1;
        drop_instance_to_bed(object, new_idx);
        plater().get_partplate_list().notify_instance_update(obj_idx, new_idx);
        plater().update();
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->increase_object_instances(size_t(obj_idx), was_one ? 2 : 1);
        plater().schedule_background_process();
    });
    return new_idx;
}

int add_volume(int obj_idx, std::vector<std::array<double, 3>> vertices, std::vector<std::array<int, 3>> triangles,
               std::string type, std::string name, std::array<double, 3> offset, std::array<double, 3> rotation,
               std::array<double, 3> scale)
{
    const ModelVolumeType vtype = volume_type_from_string(type);
    int new_idx = -1;
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: add part");
        // The mesh is given in object coordinates; keep it where it is (no recentering) and apply the
        // optional extra placement on top.
        ModelVolume* volume = object.add_volume(make_mesh(vertices, triangles), vtype, false);
        volume->set_offset(to_vec(offset));
        volume->set_rotation(to_vec(rotation));
        volume->set_scaling_factor(to_vec(scale));
        volume->name = name.empty() ? std::string("Plugin part") : name;
        int extruder = 0;
        if (vtype == ModelVolumeType::MODEL_PART && object.config.has("extruder"))
            extruder = object.config.opt_int("extruder");
        volume->config.set_key_value("extruder", new ConfigOptionInt(extruder));
        volume->source.is_from_builtin_objects = true;
        object.invalidate_bounding_box();
        if (vtype == ModelVolumeType::MODEL_PART)
            plater().get_view3D_canvas3D()->update_instance_printable_state_for_object(size_t(obj_idx));
        // sort_volumes() may move the new part; report its final index.
        refresh_object_volumes(obj_idx);
        for (size_t i = 0; i < object.volumes.size(); ++i)
            if (object.volumes[i] == volume)
                new_idx = int(i);
    });
    return new_idx;
}

void set_volume_type(int obj_idx, int vol_idx, std::string type)
{
    const ModelVolumeType vtype = volume_type_from_string(type);
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        ModelVolume& volume = volume_at(object, vol_idx);
        if (volume.is_model_part() && vtype != ModelVolumeType::MODEL_PART) {
            int solid = 0;
            for (const ModelVolume* v : object.volumes)
                solid += v->is_model_part();
            if (solid == 1)
                throw std::invalid_argument("an object needs at least one solid part");
        }
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: change part type");
        volume.set_type(vtype);
        refresh_object_volumes(obj_idx);
    });
}

void set_volume_mesh(int obj_idx, int vol_idx, std::vector<std::array<double, 3>> vertices,
                     std::vector<std::array<int, 3>> triangles, std::map<std::string, std::vector<std::string>> paint)
{
    for (auto& [layer, codes] : paint) {
        if (codes.size() != triangles.size())
            throw py::value_error("paint layer \"" + layer + "\" has " + std::to_string(codes.size()) + " codes for " +
                                  std::to_string(triangles.size()) + " triangles");
        check_paint_codes(codes);
    }
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        ModelVolume& volume = volume_at(object, vol_idx);
        for (const auto& entry : paint)
            facets_for(volume, entry.first);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: replace mesh");
        // The same sequence the Simplify gizmo uses when it replaces a mesh.
        volume.set_mesh(make_mesh(vertices, triangles));
        volume.supported_facets.reset();
        volume.seam_facets.reset();
        volume.mmu_segmentation_facets.reset();
        volume.fuzzy_skin_facets.reset();
        for (const auto& [layer, codes] : paint)
            write_paint(facets_for(volume, layer), codes);
        volume.calculate_convex_hull();
        volume.invalidate_convex_hull_2d();
        volume.set_new_unique_id();
        object.invalidate_bounding_box();
        plater().changed_mesh(obj_idx);
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list()) {
            list->update_info_items(size_t(obj_idx));
            list->update_item_error_icon(obj_idx, -1);
        }
        plater().changed_object(obj_idx);
    });
}

void set_paint(int obj_idx, int vol_idx, std::string layer, std::vector<std::string> codes)
{
    check_paint_codes(codes);
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject&      object = object_at(obj_idx);
        ModelVolume&      volume = volume_at(object, vol_idx);
        FacetsAnnotation& facets = facets_for(volume, layer);
        const size_t      n      = volume.mesh().its.indices.size();
        if (codes.size() != n)
            throw std::invalid_argument("got " + std::to_string(codes.size()) + " paint codes for " + std::to_string(n) + " triangles");
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: paint");
        write_paint(facets, codes);
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->update_info_items(size_t(obj_idx));
        plater().changed_object(obj_idx);
    });
}

std::vector<std::string> get_paint(int obj_idx, int vol_idx, std::string layer)
{
    std::vector<std::string> codes;
    PluginHostUi::run_on_ui_thread([&] {
        ModelVolume&      volume = volume_at(object_at(obj_idx), vol_idx);
        FacetsAnnotation& facets = facets_for(volume, layer);
        const size_t      n      = volume.mesh().its.indices.size();
        codes.resize(n);
        for (const auto& entry : facets.get_data().triangles_to_split)
            if (entry.triangle_idx >= 0 && size_t(entry.triangle_idx) < n)
                codes[entry.triangle_idx] = facets.get_triangle_as_string(entry.triangle_idx);
    });
    return codes;
}

std::vector<int> import_files(std::vector<std::string> paths)
{
    if (paths.empty())
        throw py::value_error("paths is empty");
    // The files are read from C++: raise the audit event a Python open(path) raises, so the plugin gets
    // the same permission check as for reading the file itself.
    for (const std::string& path : paths)
        if (PySys_Audit("open", "ssi", path.c_str(), "r", 0) < 0)
            throw py::error_already_set();
    std::vector<int> created;
    PluginHostUi::run_on_ui_thread([&] {
        std::vector<boost::filesystem::path> input;
        for (const std::string& path : paths)
            input.emplace_back(GUI::into_path(GUI::from_u8(path)));
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: import");
        // LoadModel only: geometry with its parts, painting and per-object settings, never the
        // project settings, and without the "open as project" dialog.
        for (size_t idx : plater().load_files(input, LoadStrategy::LoadModel))
            created.push_back(int(idx));
    });
    return created;
}

std::vector<int> cut(int obj_idx, int inst_idx, std::array<double, 3> point, std::array<double, 3> normal, bool keep_upper,
                     bool keep_lower, bool place_on_cut_upper, bool flip_lower, bool keep_paint)
{
    Vec3d n = to_vec(normal);
    if (n.norm() < 1e-9)
        throw py::value_error("normal is zero");
    if (!keep_upper && !keep_lower)
        throw py::value_error("keep_upper and keep_lower are both false");
    n.normalize();
    std::vector<int> created;
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject&   object = object_at(obj_idx);
        ModelInstance& inst   = instance_at(object, inst_idx);
        if (object.is_cut())
            throw std::invalid_argument("the object is already a cut part with connectors; cut it in the Cut gizmo");
        // enum_bitmask::operator|= does not compile (it assigns an enum_bitmask to the int), so use |.
        ModelObjectCutAttributes attributes;
        if (keep_upper)         attributes = attributes | ModelObjectCutAttribute::KeepUpper;
        if (keep_lower)         attributes = attributes | ModelObjectCutAttribute::KeepLower;
        if (place_on_cut_upper) attributes = attributes | ModelObjectCutAttribute::PlaceOnCutUpper;
        if (flip_lower)         attributes = attributes | ModelObjectCutAttribute::FlipLower;
        if (keep_paint)         attributes = attributes | ModelObjectCutAttribute::KeepPaint;
        // The cut plane is the XY plane of this matrix, in the instance frame without its offset (as in
        // Plater::cut_horizontal).
        Transform3d cut_matrix = Transform3d::Identity();
        cut_matrix.translate(to_vec(point) - inst.get_offset());
        cut_matrix.rotate(Eigen::Quaterniond::FromTwoVectors(Vec3d::UnitZ(), n));
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: cut");
        const size_t before = plater().model().objects.size();
        Cut          cutter(&object, inst_idx, cut_matrix, attributes);
        const auto   new_objects = cutter.perform_with_plane();
        if (new_objects.empty())
            throw std::invalid_argument("the plane does not cut the object");
        plater().apply_cut_object_to_model(size_t(obj_idx), new_objects);
        const size_t after = plater().model().objects.size();
        for (size_t i = before - 1; i < after; ++i)
            created.push_back(int(i));
    });
    return created;
}

void set_layer_ranges(int obj_idx, std::vector<std::tuple<double, double, std::map<std::string, std::string>>> ranges)
{
    // Build and validate the configs before touching the model.
    t_layer_config_ranges staged;
    for (const auto& [lo, hi, values] : ranges) {
        if (!(hi > lo))
            throw py::value_error("each layer range needs max_z > min_z");
        ModelConfig cfg;
        set_config_values(cfg, values, {});
        if (!cfg.has("layer_height"))
            throw py::value_error("each layer range needs a layer_height");
        staged.emplace(t_layer_height_range(lo, hi), std::move(cfg));
    }
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: height range modifiers");
        object.layer_config_ranges = staged;
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->update_info_items(size_t(obj_idx));
        plater().changed_object(obj_idx);
    });
}

void set_layer_height_profile(int obj_idx, std::vector<double> profile)
{
    if (profile.size() % 2 != 0)
        throw py::value_error("the layer height profile is a flat list of z, height pairs");
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: variable layer height");
        object.layer_height_profile.set(std::vector<coordf_t>(profile.begin(), profile.end()));
        if (GUI::ObjectList* list = GUI::wxGetApp().obj_list())
            list->update_info_items(size_t(obj_idx));
        plater().changed_object(obj_idx);
    });
}

void set_layer_gcodes(int plate_idx, std::vector<std::map<std::string, py::object>> items, std::string mode)
{
    CustomGCode::Info info;
    if (mode == "SingleExtruder")      info.mode = CustomGCode::SingleExtruder;
    else if (mode == "MultiAsSingle")  info.mode = CustomGCode::MultiAsSingle;
    else if (mode == "MultiExtruder")  info.mode = CustomGCode::MultiExtruder;
    else throw py::value_error("mode is SingleExtruder, MultiAsSingle or MultiExtruder");
    static const std::map<std::string, CustomGCode::Type> types = {
        {"color_change", CustomGCode::ColorChange}, {"pause", CustomGCode::PausePrint},
        {"tool_change", CustomGCode::ToolChange},   {"template", CustomGCode::Template},
        {"custom", CustomGCode::Custom}};
    for (const auto& item : items) {
        auto get = [&](const char* k) -> py::object { auto it = item.find(k); return it == item.end() ? py::none() : it->second; };
        CustomGCode::Item g;
        if (get("z").is_none())
            throw py::value_error("each item needs z");
        g.print_z = py::cast<double>(get("z"));
        const std::string type = get("type").is_none() ? "pause" : py::cast<std::string>(get("type"));
        auto t = types.find(type);
        if (t == types.end())
            throw py::value_error("unknown G-code type \"" + type + "\"");
        g.type     = t->second;
        g.extruder = get("extruder").is_none() ? 1 : py::cast<int>(get("extruder"));
        g.color    = get("color").is_none() ? std::string() : py::cast<std::string>(get("color"));
        g.extra    = get("extra").is_none() ? std::string() : py::cast<std::string>(get("extra"));
        info.gcodes.push_back(std::move(g));
    }
    std::sort(info.gcodes.begin(), info.gcodes.end(), [](const auto& a, const auto& b) { return a.print_z < b.print_z; });
    PluginHostUi::run_on_ui_thread([&] {
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: per-layer G-code");
        plater().model().plates_custom_gcodes[plate_idx] = info;
        plater().schedule_background_process();
    });
}

// Project-level settings (filament colours, flushing volumes, ...): the same path the filament colour
// picker takes (PlaterPresetComboBox::sync_colour_config).
// Groups the edits that follow into one undo step, as a gizmo session does: one snapshot now, none until the
// matching end_undo_group(). Groups nest; only the outermost pair takes effect. UI thread state only.
int g_undo_group_depth = 0;

void begin_undo_group(std::string name)
{
    PluginHostUi::run_on_ui_thread([&] {
        if (g_undo_group_depth++ == 0) {
            plater().take_snapshot(name.empty() ? std::string("Plugin: edit") : name);
            plater().suppress_snapshots();
        }
    });
}

void end_undo_group()
{
    PluginHostUi::run_on_ui_thread([] {
        if (g_undo_group_depth > 0 && --g_undo_group_depth == 0)
            plater().allow_snapshots();
    });
}

// Brim ears as the Brim Ears gizmo stores them: x, y, z in object coordinates and the ear radius. An empty list
// removes them. The object's brim_type is left to set_config.
void set_brim_ears(int obj_idx, std::vector<std::array<double, 4>> points)
{
    PluginHostUi::run_on_ui_thread([&] {
        ModelObject& object = object_at(obj_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: brim ears");
        object.brim_points.clear();
        for (const auto& p : points)
            object.brim_points.emplace_back(float(p[0]), float(p[1]), float(p[2]), float(p[3]));
        plater().set_plater_dirty(true);
        plater().changed_object(obj_idx);
    });
}

std::vector<std::array<double, 4>> get_brim_ears(int obj_idx)
{
    std::vector<std::array<double, 4>> out;
    PluginHostUi::run_on_ui_thread([&] {
        for (const BrimPoint& p : object_at(obj_idx).brim_points)
            out.push_back({p.pos.x(), p.pos.y(), p.pos.z(), p.head_front_radius});
    });
    return out;
}

// The plate settings dialog. print_sequence and spiral_mode accept "default" to follow the global setting.
void set_plate(int plate_idx, std::optional<std::string> name, std::optional<std::string> bed_type,
               std::optional<std::string> print_sequence, std::optional<std::string> spiral_mode, std::optional<bool> locked)
{
    std::optional<BedType> bt;
    if (bed_type) {
        const t_config_enum_values& names = ConfigOptionEnum<BedType>::get_enum_values();
        const auto                  it    = names.find(*bed_type);
        if (it == names.end())
            throw py::value_error("unknown bed type \"" + *bed_type + "\"");
        bt = BedType(it->second);
    }
    std::optional<PrintSequence> seq;
    if (print_sequence) {
        if (*print_sequence == "default")
            seq = PrintSequence::ByDefault;
        else {
            const t_config_enum_values& names = ConfigOptionEnum<PrintSequence>::get_enum_values();
            const auto                  it    = names.find(*print_sequence);
            if (it == names.end())
                throw py::value_error("unknown print sequence \"" + *print_sequence + "\"");
            seq = PrintSequence(it->second);
        }
    }
    if (spiral_mode && *spiral_mode != "default" && *spiral_mode != "1" && *spiral_mode != "0")
        throw py::value_error("spiral_mode must be \"1\", \"0\" or \"default\"");

    PluginHostUi::run_on_ui_thread([&] {
        GUI::PartPlateList& plates = plater().get_partplate_list();
        if (plate_idx < 0 || plate_idx >= plates.get_plate_count())
            throw py::index_error("plate index " + std::to_string(plate_idx) + " out of range");
        GUI::PartPlate* plate = plates.get_plate(plate_idx);
        GUI::Plater::TakeSnapshot snapshot(&plater(), "Plugin: plate settings");
        if (bt)
            plate->set_bed_type(*bt);
        if (seq)
            plate->set_print_seq(*seq);
        if (spiral_mode)
            plate->set_spiral_vase_mode(*spiral_mode == "1", *spiral_mode == "default");
        if (locked)
            plate->lock(*locked);
        if (name)
            plater().rename_plate(plate_idx, *name);
        plater().update_project_dirty_from_presets();
        plater().set_plater_dirty(true);
        DynamicPrintConfig plate_config = *plate->config();
        plater().config_change_notification(plate_config, std::string("print_sequence"));
        plater().update();
    });
}

// The project information stored in the 3MF (Title, Designer, Description, License, CopyRight, Origin).
void set_project_info(std::map<std::string, std::string> values)
{
    static const char* const keys[] = {"title", "designer", "description", "license", "copyright", "origin"};
    for (const auto& [key, value] : values)
        if (std::find_if(std::begin(keys), std::end(keys), [&](const char* k) { return key == k; }) == std::end(keys))
            throw py::value_error("unknown project info \"" + key + "\"");
    PluginHostUi::run_on_ui_thread([&] {
        Model& model = plater().model();
        if (!model.model_info)
            model.model_info = std::make_shared<ModelInfo>();
        ModelInfo& info = *model.model_info;
        for (const auto& [key, value] : values) {
            if (key == "title")
                info.model_name = value;
            else if (key == "designer")
                model.SetDesigner(value, "");
            else if (key == "description")
                info.description = value;
            else if (key == "license")
                info.license = value;
            else if (key == "copyright")
                info.copyright = value;
            else if (key == "origin")
                info.origin = value;
        }
        plater().set_plater_dirty(true);
    });
}

void set_project_config(std::map<std::string, std::string> values)
{
    DynamicPrintConfig staged;
    ConfigSubstitutionContext ctx(ForwardCompatibilitySubstitutionRule::Disable);
    for (const auto& [key, value] : values) {
        if (print_config_def.get(key) == nullptr)
            throw py::value_error("unknown setting \"" + key + "\"");
        staged.set_deserialize(key, value, ctx);
    }
    PluginHostUi::run_on_ui_thread([&] {
        DynamicPrintConfig& project = GUI::wxGetApp().preset_bundle->project_config;
        for (const auto& [key, value] : values)
            if (!project.has(key))
                throw std::invalid_argument("\"" + key + "\" is not a project setting");
        DynamicPrintConfig cfg_new = project;
        cfg_new.apply(staged);
        // A plain colour change keeps the multi-colour list and colour type in step, as the picker does.
        if (staged.has("filament_colour")) {
            const auto& colours = staged.option<ConfigOptionStrings>("filament_colour")->values;
            if (auto* multi = cfg_new.option<ConfigOptionStrings>("filament_multi_colour")) {
                if (multi->values.size() < colours.size())
                    multi->values.resize(colours.size());
                for (size_t i = 0; i < colours.size(); ++i)
                    if (multi->values[i].find(' ') == std::string::npos)
                        multi->values[i] = colours[i];
            }
            if (auto* types = cfg_new.option<ConfigOptionStrings>("filament_colour_type"))
                if (types->values.size() < colours.size())
                    types->values.resize(colours.size(), "1");
        }
        project.apply(cfg_new);
        plater().update_project_dirty_from_presets();
        plater().sidebar().update_all_preset_comboboxes();
        plater().on_config_change(cfg_new);
    });
}

// The filament panel's "+" button: adds a physical filament with the given colour; returns the new count.
int add_filament(std::string colour)
{
    int count = 0;
    PluginHostUi::run_on_ui_thread([&] {
        const wxColour col(wxString::FromUTF8(colour));
        if (!col.IsOk())
            throw std::invalid_argument("invalid colour \"" + colour + "\"");
        auto* bundle = GUI::wxGetApp().preset_bundle;
        const size_t before = bundle->filament_presets.size();
        plater().sidebar().add_custom_filament(col);
        count = int(bundle->num_physical_filaments());
        if (bundle->filament_presets.size() == before)
            throw std::runtime_error("no filament was added (the maximum number of filaments may be reached)");
    });
    return count;
}

// Run a Python callable on the UI thread and return its result: the host's own marshaling, so a plugin
// can read the model or export the project from its script thread without a UI page to pump calls.
py::object call_on_ui(py::function fn)
{
    py::object result, err_type, err_value, err_trace;
    PluginHostUi::run_on_ui_thread([&] {
        py::gil_scoped_acquire gil;
        try {
            result = fn();
        } catch (py::error_already_set& e) {
            err_type  = e.type();
            err_value = e.value();
            err_trace = e.trace();
        }
    });
    if (err_type) {
        PyErr_Restore(err_type.release().ptr(), err_value.release().ptr(), err_trace.release().ptr());
        throw py::error_already_set();
    }
    return result;
}

} // namespace

void host_bindings::register_edit(py::module_& host)
{
    auto edit = host.def_submodule(
        "edit",
        "Write access to the live project: transforms, settings, parts, painting, cut, layer ranges and "
        "per-layer G-code, each recorded as one undo step and refreshed like a manual edit. Callable from the "
        "UI thread or the script thread (calls are marshaled to the UI thread). Not from a slicing hook.");

    edit.def("set_instance_transform", &set_instance_transform, py::arg("object"), py::arg("instance") = 0,
             py::arg("offset") = py::none(), py::arg("rotation") = py::none(), py::arg("scale") = py::none(),
             py::arg("mirror") = py::none(), py::arg("drop_to_bed") = true,
             "Set an instance's offset (mm), rotation (radians, XYZ), scaling factor and mirror; None keeps a value.");
    edit.def("set_volume_transform", &set_volume_transform, py::arg("object"), py::arg("volume"),
             py::arg("offset") = py::none(), py::arg("rotation") = py::none(), py::arg("scale") = py::none(),
             py::arg("mirror") = py::none(), "Set a part's transform within its object; None keeps a value.");
    edit.def("set_instance_matrix", &set_instance_matrix, py::arg("object"), py::arg("instance"), py::arg("matrix"),
             "Set an instance's whole object-to-bed transform: 16 numbers, row-major 4x4.");
    edit.def("set_volume_matrix", &set_volume_matrix, py::arg("object"), py::arg("volume"), py::arg("matrix"),
             "Set a part's whole part-to-object transform: 16 numbers, row-major 4x4.");
    edit.def("set_config", &set_config, py::arg("object"), py::arg("volume") = -1,
             py::arg("values") = std::map<std::string, std::string>(), py::arg("erase") = std::vector<std::string>(),
             "Set (serialized values) or erase per-object (volume=-1) or per-part settings.");
    edit.def("rename", &rename, py::arg("object"), py::arg("volume") = -1, py::arg("name"));
    edit.def("set_printable", &set_printable, py::arg("object"), py::arg("instance"), py::arg("printable"));
    edit.def("delete_object", &delete_object, py::arg("object"));
    edit.def("delete_volume", &delete_volume, py::arg("object"), py::arg("volume"));
    edit.def("delete_instance", &delete_instance, py::arg("object"), py::arg("instance"));
    edit.def("add_instance", &add_instance, py::arg("object"), py::arg("offset"), py::arg("rotation") = py::none(),
             py::arg("scale") = py::none(), "Add a copy at offset (mm); returns its index.");
    edit.def(
        "add_volume",
        [](int obj, py::object vertices, py::object triangles, std::string type, std::string name,
           std::array<double, 3> offset, std::array<double, 3> rotation, std::array<double, 3> scale) {
            auto v = to_vertices(vertices);
            auto t = to_triangles(triangles, v.size());
            return add_volume(obj, std::move(v), std::move(t), std::move(type), std::move(name), offset, rotation, scale);
        },
        py::arg("object"), py::arg("vertices"), py::arg("triangles"), py::arg("type") = "modifier", py::arg("name") = "",
        py::arg("offset") = std::array<double, 3>{0, 0, 0}, py::arg("rotation") = std::array<double, 3>{0, 0, 0},
        py::arg("scale") = std::array<double, 3>{1, 1, 1},
        "Add a part (part, negative, modifier, support_enforcer, support_blocker) from a mesh in object "
        "coordinates; returns its index after the object list reorders the parts.");
    edit.def("set_volume_type", &set_volume_type, py::arg("object"), py::arg("volume"), py::arg("type"));
    edit.def(
        "set_volume_mesh",
        [](int obj, int vol, py::object vertices, py::object triangles, std::map<std::string, std::vector<std::string>> paint) {
            auto v = to_vertices(vertices);
            auto t = to_triangles(triangles, v.size());
            set_volume_mesh(obj, vol, std::move(v), std::move(t), std::move(paint));
        },
        py::arg("object"), py::arg("volume"), py::arg("vertices"), py::arg("triangles"),
        py::arg("paint") = std::map<std::string, std::vector<std::string>>(),
        "Replace a part's mesh (volume coordinates), optionally with painting per layer (color, supports, seam, "
        "fuzzy_skin) as one 3MF paint code per triangle; other painting is cleared.");
    edit.def("set_paint", &set_paint, py::arg("object"), py::arg("volume"), py::arg("layer"), py::arg("codes"),
             "Replace one painting layer of a part: one 3MF paint code per triangle, \"\" for unpainted.");
    edit.def("get_paint", &get_paint, py::arg("object"), py::arg("volume"), py::arg("layer"),
             "One 3MF paint code per triangle (\"\" = unpainted) for a painting layer of a part.");
    edit.def("import_files", &import_files, py::arg("paths"),
             "Add the objects of model files (3MF, STL, OBJ, STEP, ...) to the plate without dialogs and without "
             "loading project settings; returns the new object indices.");
    edit.def("cut", &cut, py::arg("object"), py::arg("instance") = 0, py::arg("point"), py::arg("normal"),
             py::arg("keep_upper") = true, py::arg("keep_lower") = true, py::arg("place_on_cut_upper") = true,
             py::arg("flip_lower") = false, py::arg("keep_paint") = true,
             "Cut an object with the plane through point (bed coordinates) with the given normal, like the Cut "
             "gizmo's plane cut; returns the indices of the resulting objects.");
    edit.def("set_layer_ranges", &set_layer_ranges, py::arg("object"), py::arg("ranges"),
             "Replace the height range modifiers: [(min_z, max_z, {key: value, ...}), ...]; each needs layer_height.");
    edit.def("set_layer_height_profile", &set_layer_height_profile, py::arg("object"), py::arg("profile"),
             "Variable layer height: flat [z0, h0, z1, h1, ...]; an empty list removes it.");
    edit.def("set_layer_gcodes", &set_layer_gcodes, py::arg("plate"), py::arg("items"), py::arg("mode") = "SingleExtruder",
             "Replace a plate's per-layer G-code (0-based plate): items are dicts with z, type (pause, color_change, "
             "tool_change, template, custom), extruder, color, extra.");
    edit.def("set_project_config", &set_project_config, py::arg("values"),
             "Set project settings (serialized values), e.g. filament_colour or flush_volumes_matrix.");
    edit.def("add_filament", &add_filament, py::arg("colour"),
             "Add a filament with this colour (#RRGGBB), like the filament panel's + button; returns the count.");
    edit.def("begin_undo_group", &begin_undo_group, py::arg("name") = "",
             "Group the following edits into one undo step until end_undo_group().");
    edit.def("end_undo_group", &end_undo_group);
    edit.def("set_brim_ears", &set_brim_ears, py::arg("object"), py::arg("points"),
             "Brim ears [x, y, z, radius] in object coordinates; [] removes them.");
    edit.def("get_brim_ears", &get_brim_ears, py::arg("object"));
    edit.def("set_plate", &set_plate, py::arg("plate"), py::arg("name") = py::none(), py::arg("bed_type") = py::none(),
             py::arg("print_sequence") = py::none(), py::arg("spiral_mode") = py::none(), py::arg("locked") = py::none(),
             "Plate settings (0-based plate); print_sequence/spiral_mode accept \"default\".");
    edit.def("set_project_info", &set_project_info, py::arg("values"),
             "Project info: title, designer, description, license, copyright, origin.");
    edit.def("call_on_ui", &call_on_ui, py::arg("fn"),
             "Call fn() on the UI thread and return its result (exceptions propagate). For reading the model, "
             "exporting or anything else that needs the UI thread, from the script thread.");
    edit.def("arrange", [] { PluginHostUi::run_on_ui_thread([] { plater().arrange(); }); }, "Arrange the plate (as the toolbar).");
    edit.def("orient", [] { PluginHostUi::run_on_ui_thread([] { plater().orient(); }); }, "Auto-orient (as the toolbar).");
    edit.def("reslice", [] { PluginHostUi::run_on_ui_thread([] { plater().reslice(); }); }, "Slice the current plate.");
    edit.def("undo", [] { PluginHostUi::run_on_ui_thread([] { plater().undo(); }); });
    edit.def("redo", [] { PluginHostUi::run_on_ui_thread([] { plater().redo(); }); });
}

} // namespace Slic3r
