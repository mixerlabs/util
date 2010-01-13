"""rectangle utilities"""


def int_div_round(numerator, denominator):
    return (numerator + denominator / 2) / denominator


def largest_subrect_size_keep_aspect(inner_size, outer_size):
    """ Returns the dimensions of the largest rectangle of aspect
    ratio inner_size that will fit inside of outer_size."""
    width = int_div_round(outer_size[0] * inner_size[1], outer_size[1])
    if width <= inner_size[0]:
        return (width, inner_size[1])
    height = int_div_round(outer_size[1] * inner_size[0], outer_size[0])
    return (inner_size[0], height)


def contain_subrect_around_point(inner_size, outer_size, center_point):
    """ Returns (left, top, right, bottom) of a rectangle of inner_size
    centered as close to center_point as possible while keeping inside
    of the rect specified by outer_size"""
    (left, top) = (center_point[0] - inner_size[0] / 2,
                   center_point[1] - inner_size[1] / 2)
    (right, bottom) = (left + inner_size[0], top + inner_size[1])
    if left < 0:
        right -= left
        left = 0
    if top < 0:
        bottom -= top
        top = 0
    if right > outer_size[0]:
        left -= right - outer_size[0]
        right = outer_size[0]
    if bottom > outer_size[1]:
        top -= bottom - outer_size[1]
        bottom = outer_size[1]
    return (left, top, right, bottom)


def scale_dimensions(constrain_size, original_size):
    """ Returns a scaled version of original_size constrained to whichever
    dimension is specified in constrain_size. Sizes are (width, height)"""
    if constrain_size[1] is None:
        if constrain_size[0] is None:
            return original_size
        else:
            return (constrain_size[0], int_div_round(
                  constrain_size[0] * original_size[1], original_size[0]))
    elif constrain_size[0] is None:
            return (int_div_round(constrain_size[1] * original_size[0],
                    original_size[1]), constrain_size[1])
    return constrain_size

