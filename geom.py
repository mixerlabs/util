"""Utilities for wrangling geometries."""

from __future__ import with_statement
from __future__ import absolute_import

from django.contrib.gis.geos import (Point, Polygon,
                                     MultiPolygon,
                                     GEOSGeometry as Geometry)


def polygonize(geom):
    """Try really hard to make the given geometry a polygon."""
    strategies = [
        lambda g: g.simplify(),
        lambda g: g.simplify(0.0001),
        lambda g: g.buffer(0),
        lambda g: g.convex_hull,
    ]

    for strategy in strategies:
        if isinstance(geom, Polygon):
            return geom

        geom = strategy(geom)
    else:
        raise ValueError, 'Unable to make a polygon of it!!'

# From http://www.faqs.org/faqs/graphics/algorithms-faq/
# 
# * Subject 1.02: How do I find the distance from a point to a line?
#
# Let the point be C (Cx,Cy) and the line be AB (Ax,Ay) to
# (Bx,By). Let P be the point of perpendicular projection of C on
# AB. The parameter r, which indicates P's position along AB, is
# computed by the dot product of AC and AB divided by the square of
# the length of AB:
#
# (1)      AC dot AB 
#      r = ---------
#          ||AB||^2

def closest_point_on_line(point, line):
    """Return the closest point (to `point') on `line' (a two-tuple of points"""
    p0, p1 = line

    dx = p1.x - p0.x
    dy = p1.y - p0.y

    r = ((point.x - p0.x)*dx + (point.y - p0.y)*dy) / (dx*dx + dy*dy)
    r = min(1, max(0, r))

    return Point(p0.x + r*dx, p0.y + r*dy)
