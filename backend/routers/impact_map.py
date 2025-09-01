"""API routes for impact map management."""

from fastapi import APIRouter, HTTPException

try:
    from ..schemas import ImpactMapPost, ImpactMapActiveOut
    from ..services.impact_map_service import create_new_impact_map, get_active_map
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from schemas import ImpactMapPost, ImpactMapActiveOut
    from services.impact_map_service import create_new_impact_map, get_active_map


router = APIRouter(tags=["impact-map"])


@router.post("/impact-map")
def post_impact_map(payload: ImpactMapPost):
    """
    Create a new impact map version.
    
    Accepts JSON map of aspect→category weights, validates planets/aspects/categories,
    and stores both the original JSON and exploded rules.
    """
    try:
        version_id = create_new_impact_map(payload)
        return {"version_id": version_id, "status": "ok"}
    except ValueError as ve:
        raise HTTPException(status_code=422, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/impact-map/active", response_model=ImpactMapActiveOut)
def get_active():
    """
    Get the active impact map version.
    
    Returns the version_id, created_at, and original JSON map for the currently active version.
    """
    try:
        data = get_active_map()
        return {
            "version_id": data["version_id"],
            "created_at": data["created_at"],
            "notes": data.get("notes"),
            "map": data["map"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))