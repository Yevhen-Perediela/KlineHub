from fastapi import APIRouter, Request

from ..schemas import RefreshPopularPairsRequest, RefreshPopularPairsResponse

router = APIRouter(prefix="/internal", tags=["internal"])


@router.post("/refresh-popular-pairs", response_model=RefreshPopularPairsResponse)
async def refresh_popular_pairs(
    payload: RefreshPopularPairsRequest,
    request: Request,
) -> RefreshPopularPairsResponse:
    return await request.app.state.popular_pairs_service.refresh_popular_pairs(payload=payload)
