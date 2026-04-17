def wrap_frame(
    text: str,
    min_width: int = 79,
) -> str:
    """Wraps text in a frame with a minimum size.

    Arguments:
           text (str): Text to wrap
           min_width (int): Minimum frame width (default 79)

    Returns:
           str: Text in frame

    """

    lines = [line.strip() for line in str(text).split("\n") if line.strip()]
    max_line_length = max(len(line) for line in lines) if lines else 0
    content_width = max(
        max_line_length, min_width - 4,
    )
    frame_width = content_width + 4
    result = [""]
    result.append("┌" + "─" * (frame_width - 2) + "┐")

    for line in lines:
        spaces_needed = content_width - len(line)
        padded_line = f" {line}{' ' * spaces_needed} "
        result.append("│" + padded_line + "│")

    result.append("└" + "─" * (frame_width - 2) + "┘")
    return "\n".join(result)
