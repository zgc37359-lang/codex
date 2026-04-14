use codex_protocol::models::FunctionCallOutputContentItem;
use codex_protocol::models::ImageDetail;

pub(crate) use codex_tools::can_request_original_image_detail;
pub(crate) use codex_tools::normalize_output_image_detail;

pub(crate) fn sanitize_original_image_detail(
    can_request_original_image_detail: bool,
    items: &mut [FunctionCallOutputContentItem],
) {
    if can_request_original_image_detail {
        return;
    }

    for item in items {
        if let FunctionCallOutputContentItem::InputImage { detail, .. } = item
            && matches!(detail, Some(ImageDetail::Original))
        {
            *detail = None;
        }
    }
}
