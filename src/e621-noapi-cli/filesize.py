def bytes_to_megabytes(num_bytes: int, round_decimal_points: int = -1) -> float:
    """
    Converts a size in bytes (B) to a size in megabytes (MB), optionally rounding the returned float value.
    :param num_bytes: The size in bytes
    :param round_decimal_points: If >= 0, the number of decimal points to round to
    """
    mb = num_bytes / 1024 / 1024
    if round_decimal_points >= 0:
        mb = round(mb, round_decimal_points)
    return mb


def bytes_to_gigabytes(num_bytes: int, round_decimal_points: int = -1) -> float:
    """
    Converts a size in bytes (B) to a size in gigabytes (GB), optionally rounding the returned float value.
    :param num_bytes: The size in bytes
    :param round_decimal_points: If >= 0, the number of decimal points to round to
    """
    gb = bytes_to_megabytes(num_bytes) / 1024
    if round_decimal_points >= 0:
        gb = round(gb, round_decimal_points)
    return gb
