import os
from dotenv import load_dotenv
import supervisely as sly


def tag_dataset(
    api: sly.Api,
    dataset: sly.DatasetInfo,
    project_id: int,
    project_meta: sly.ProjectMeta,
):
    """Adds dataset name tag to all images in dataset"""

    # Get or Create TagMeta
    tag_meta = project_meta.get_tag_meta(dataset.name)
    if tag_meta is None:
        tag_meta = sly.TagMeta(dataset.name, sly.TagValueType.NONE)
        project_meta = project_meta.add_tag_meta(tag_meta)
        api.project.update_meta(project_id, project_meta)
    if tag_meta.value_type != sly.TagValueType.NONE:
        sly.logger.error(
            "TagMeta already exist in ProjectMeta but TagValueType is not NONE. Wrong TagValueType: %s",
            tag_meta.value_type,
            exc_info=1,
        )
        raise ValueError(tag_meta.value_type)

    # Create Tag
    tag = sly.Tag(tag_meta, value=None)

    # Get Images Ids and Annotations
    img_ids = [img.id for img in api.image.get_list(dataset.id)]
    ann_jsons = api.annotation.download_json_batch(dataset.id, img_ids)
    anns = [sly.Annotation.from_json(ann_json, project_meta) for ann_json in ann_jsons]

    # Update Annotations
    anns = [ann if tag in ann.img_tags else ann.add_tag(tag) for ann in anns]

    # Upload updated annotations
    api.annotation.upload_anns(img_ids, anns)


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
        for dataset in api.dataset.get_list(project_id):
            tag_dataset(api, dataset, project_id, project_meta)
    else:
        dataset = api.dataset.get_info_by_id(dataset_id)
        tag_dataset(api, dataset, project_id, project_meta)
