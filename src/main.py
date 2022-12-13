import os
from typing import Literal
from dotenv import load_dotenv
import supervisely as sly
from supervisely._utils import batched
from supervisely.annotation.tag_collection import TagCollection


def get_or_create_tag_meta(
    api: sly.Api,
    project_meta: sly.ProjectMeta,
    tag_name: str,
    tag_value_type: str,
):
    tag_meta = project_meta.get_tag_meta(tag_name)
    if tag_meta is None:
        tag_meta = sly.TagMeta(tag_name, tag_value_type)
        project_meta = project_meta.add_tag_meta(tag_meta)
        api.project.update_meta(dataset.project_id, project_meta)
        sly.logger.info(f'Added TagMeta "{tag_name}" with ValueType "{tag_value_type}"')
        project_meta_json = api.project.get_meta(dataset.project_id)
        project_meta = sly.ProjectMeta.from_json(project_meta_json)
        tag_meta = project_meta.get_tag_meta(tag_name)
    if tag_meta.value_type != tag_value_type:
        raise ValueError(
            f'TagMeta value type is not "{sly.TagValueType.NONE}". Wrong TagValueType: "{tag_meta.value_type}"'
        )
    return tag_meta


def add_tag_to_dataset(
    api: sly.Api,
    dataset: sly.DatasetInfo,
    project_meta: sly.ProjectMeta,
    tag_meta: sly.TagMeta,
    batch_size: int = 100,
    progress: sly.Progress = None,
):
    images = api.image.get_list(dataset.id)
    for batch in batched(images, batch_size):
        img_ids = []
        for image_info in batch:
            tags = TagCollection.from_api_response(
                image_info.tags, project_meta.tag_metas
            )
            if not tags.has_key(tag_meta.name):
                img_ids.append(image_info.id)
        api.image.add_tag_batch(
            img_ids, tag_meta.sly_id, batch_size=batch_size, tag_meta=tag_meta
        )
        if not progress is None:
            progress.set_current_value(progress.current + len(batch))


def tag_dataset(
    api: sly.Api,
    dataset: sly.DatasetInfo,
    batch_size: int = 100,
    progress: sly.Progress = None,
):
    """Adds dataset name tag to all images in dataset"""

    # Get project_meta
    project_meta_json = api.project.get_meta(dataset.project_id)
    project_meta = sly.ProjectMeta.from_json(project_meta_json)

    # Get or Create TagMeta
    try:
        tag_meta = get_or_create_tag_meta(
            api, project_meta, dataset.name, sly.TagValueType.NONE
        )
    except ValueError:
        sly.logger.warning(
            f'TagMeta "{dataset.name}" already exist in ProjectMeta but TagValueType is not "{sly.TagValueType.NONE}". Wrong TagValueType: "{tag_meta.value_type}". Skipping dataset...'
        )
        return

    # Add tag to all images in dataset
    add_tag_to_dataset(api, dataset, project_meta, tag_meta, batch_size, progress)


if __name__ == "__main__":
    if sly.is_development():
        load_dotenv("local.env")
        load_dotenv(os.path.expanduser("~/supervisely.env"))

    api: sly.Api = sly.Api.from_env()
    project_id = sly.env.project_id()

    dataset_id = sly.env.dataset_id(False)
    if dataset_id is None:
        datasets = api.dataset.get_list(project_id)
        total_imgs = 0
        for dataset in datasets:
            total_imgs += dataset.images_count
        progress = sly.Progress("Processing", total_imgs)
        for dataset in datasets:
            tag_dataset(api, dataset, progress=progress)
    else:
        dataset = api.dataset.get_info_by_id(dataset_id)
        total_imgs = dataset.images_count
        progress = sly.Progress("Processing", total_imgs)
        tag_dataset(api, dataset, progress=progress)

    if sly.is_production():
        task_id = sly.env.task_id()
        info = api.project.get_info_by_id(project_id)
        api.task.set_output_project(task_id=task_id, project_id=project_id)
        print(f"Result project: id={project_id}, name={info.name}")
