import os
from dotenv import load_dotenv
import supervisely as sly
from supervisely._utils import batched
from supervisely.annotation.tag_collection import TagCollection


def tag_dataset(
    api: sly.Api,
    dataset: sly.DatasetInfo,
    project_id: int,
    project_meta: sly.ProjectMeta,
    batch_size: int = 100,
    progress: sly.Progress = None,
):
    """Adds dataset name tag to all images in dataset"""

    # Get or Create TagMeta
    tag_meta = project_meta.get_tag_meta(dataset.name)
    if tag_meta is None:
        tag_meta = sly.TagMeta(dataset.name, sly.TagValueType.NONE)
        project_meta = project_meta.add_tag_meta(tag_meta)
        api.project.update_meta(project_id, project_meta)
        project_meta_json = api.project.get_meta(project_id)
        project_meta = sly.ProjectMeta.from_json(project_meta_json)
        tag_meta = project_meta.get_tag_meta(dataset.name)
    if tag_meta.value_type != sly.TagValueType.NONE:
        sly.logger.warning(
            f'TagMeta already exist in ProjectMeta but TagValueType is not "{sly.TagValueType.NONE}". Wrong TagValueType: "{tag_meta.value_type}"'
        )
        return

    images = api.image.get_list(dataset.id)
    for batch in batched(images, batch_size):
        img_ids = []
        for image_info in batch:
            tags = TagCollection.from_api_response(
                image_info.tags, project_meta.tag_metas
            )
            if not tags.has_key(tag_meta.name):
                img_ids.append(image_info.id)
        api.image.add_tag_batch(img_ids, tag_meta.sly_id, batch_size=batch_size)
        if not progress is None:
            progress.set_current_value(progress.current+len(batch))


if __name__ == "__main__":
    if sly.is_development():
        load_dotenv("local.env")
        load_dotenv(os.path.expanduser("~/supervisely.env"))

    api: sly.Api = sly.Api.from_env()
    project_id = sly.env.project_id()
    project_meta_json = api.project.get_meta(project_id)
    project_meta = sly.ProjectMeta.from_json(project_meta_json)

    dataset_id = sly.env.dataset_id(False)
    if dataset_id is None:
        datasets = api.dataset.get_list(project_id)
        total_imgs = 0
        for dataset in datasets:
            total_imgs += dataset.images_count
        progress = sly.Progress("Processing", total_imgs)
        for dataset in datasets:
            tag_dataset(api, dataset, project_id, project_meta, progress=progress)
    else:
        dataset = api.dataset.get_info_by_id(dataset_id)
        total_imgs = dataset.images_count
        progress = sly.Progress("Processing", total_imgs)
        tag_dataset(api, dataset, project_id, project_meta, progress=progress)
    
    if sly.is_production():
        task_id = sly.env.task_id()
        info = api.project.get_info_by_id(project_id)
        api.task.set_output_project(task_id=task_id, project_id=project_id)
        print(f"Result project: id={project_id}, name={info.name}")
