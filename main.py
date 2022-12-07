import os
from dotenv import load_dotenv 
import supervisely as sly


def tag_dataset(dataset: sly.DatasetInfo, project_id: int, project_meta: sly.ProjectMeta, api: sly.Api):
    """Adds dataset name tag to all images in dataset"""
    TAG_TYPES_NULL_VALUES = {
        'any_number': 0,
        'any_string': '',
        'none': None,
    }

    tag_meta = project_meta.get_tag_meta(dataset.name)
    # If tag meta does not exist we create new
    if tag_meta is None:
        tag_meta = sly.TagMeta(dataset.name, sly.TagValueType.NONE)
        project_meta = project_meta.add_tag_meta(tag_meta)
        api.project.update_meta(project_id, project_meta)

    # Get tag value based on tag meta type
    if tag_meta.possible_values:
        tag_value = tag_meta.possible_values[0]
    else:
        tag_value = TAG_TYPES_NULL_VALUES[tag_meta.value_type]

    # create tag
    tag = sly.Tag(tag_meta, value=tag_value)
    
    # load/create annotation for each image
    anns = api.annotation.get_list(dataset.id)
    img_ann = {ann.image_id:ann for ann in anns}
    for image_info in api.image.get_list(dataset.id):
        ann = img_ann.get(image_info.id, None)
        if ann is None:
            ann = sly.Annotation(image_info.size, img_tags=[tag])
        else:
            ann = sly.Annotation.from_json(ann.annotation, project_meta)
            if not tag in ann.img_tags:
                ann = ann.add_tag(tag)
        img_ann[image_info.id] = ann

    # bulk upload annotations for all images
    api.annotation.upload_anns(*zip(*img_ann.items()))


if __name__ == '__main__':
    if sly.is_development():
        load_dotenv("local.env")
        load_dotenv(os.path.expanduser("~/supervisely.env"))

    api: sly.Api = sly.Api.from_env()
    project_id = sly.env.project_id()
    project_meta = api.project.get_meta(project_id)
    project_meta = sly.ProjectMeta.from_json(project_meta)

    dataset_id = sly.env.dataset_id()
    if dataset_id is None:
        for dataset in api.dataset.get_list(project_id):
            tag_dataset(dataset, project_id, project_meta, api)
    else:
        dataset = api.dataset.get_info_by_id(dataset_id)
        tag_dataset(dataset, project_id, project_meta, api)

