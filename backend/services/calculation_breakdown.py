"""Detailed calculation breakdown service for opportunity analysis."""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import math

try:
    from ..supabase_client import supabase
    from ..config import settings
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from supabase_client import supabase
    from config import settings

logger = logging.getLogger(__name__)


class CalculationStep:
    """Individual calculation step in the breakdown."""
    
    def __init__(self, name: str, formula: str, inputs: Dict[str, Any], output: float, explanation: str):
        self.name = name
        self.formula = formula
        self.inputs = inputs
        self.output = output
        self.explanation = explanation
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "formula": self.formula,
            "inputs": self.inputs,
            "output": self.output,
            "explanation": self.explanation
        }


class OpportunityBreakdown:
    """Complete breakdown of opportunity calculation."""
    
    def __init__(self, opportunity_id: str, market_data: Dict[str, Any]):
        self.opportunity_id = opportunity_id
        self.market_data = market_data
        self.steps: List[CalculationStep] = []
        self.aspects_analysis: List[Dict[str, Any]] = []
        self.risk_analysis: Dict[str, Any] = {}
        self.final_summary: Dict[str, Any] = {}
    
    def add_step(self, step: CalculationStep):
        """Add a calculation step."""
        self.steps.append(step)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "opportunity_id": self.opportunity_id,
            "market_data": self.market_data,
            "calculation_steps": [step.to_dict() for step in self.steps],
            "aspects_analysis": self.aspects_analysis,
            "risk_analysis": self.risk_analysis,
            "final_summary": self.final_summary
        }


async def get_opportunity_calculation_breakdown(opportunity_id: str) -> Dict[str, Any]:
    """
    Get detailed calculation breakdown for an opportunity.
    
    Args:
        opportunity_id: UUID of the opportunity
        
    Returns:
        Complete calculation breakdown
    """
    try:
        # Get opportunity data
        opportunities = await supabase.select(
            table="opportunities",
            select="*",
            eq={"id": opportunity_id}
        )
        
        if not opportunities:
            raise ValueError(f"Opportunity {opportunity_id} not found")
        
        opp = opportunities[0]
        
        # Get market data
        market_data = await _get_market_data(opp["market_id"])
        
        # Get aspect contributions
        contributions = await _get_aspect_contributions(opp["market_id"])
        
        # Build breakdown
        breakdown = OpportunityBreakdown(opportunity_id, market_data)
        
        # Step 1: Base probability
        breakdown.add_step(CalculationStep(
            name="Base Probability",
            formula="p0 = market_price",
            inputs={"market_price": opp["p0"]},
            output=opp["p0"],
            explanation="Starting probability based on current market price"
        ))
        
        # Step 2: Astrology score calculation
        s_astro = opp["s_astro"]
        breakdown.add_step(CalculationStep(
            name="Astrology Score",
            formula="s_astro = Σ(aspect_contributions)",
            inputs={"contributions": len(contributions)},
            output=s_astro,
            explanation=f"Sum of {len(contributions)} aspect contributions"
        ))
        
        # Step 3: Adjusted probability
        p_astro = opp["p_astro"]
        breakdown.add_step(CalculationStep(
            name="Astrology-Adjusted Probability",
            formula="p_astro = sigmoid(logit(p0) + s_astro)",
            inputs={
                "p0": opp["p0"],
                "s_astro": s_astro,
                "logit_p0": _logit(opp["p0"]),
                "adjusted_logit": _logit(opp["p0"]) + s_astro
            },
            output=p_astro,
            explanation="Probability adjusted by astrological influences"
        ))
        
        # Step 4: Raw edge calculation
        raw_edge = abs(p_astro - opp["p0"])
        breakdown.add_step(CalculationStep(
            name="Raw Edge",
            formula="raw_edge = |p_astro - p0|",
            inputs={"p_astro": p_astro, "p0": opp["p0"]},
            output=raw_edge,
            explanation="Absolute difference between adjusted and base probability"
        ))
        
        # Step 5: Cost calculation
        costs = opp.get("costs", {})
        total_costs = sum(costs.values()) if isinstance(costs, dict) else 0.02  # Default
        breakdown.add_step(CalculationStep(
            name="Total Costs",
            formula="costs = fee + spread + slippage",
            inputs=costs if isinstance(costs, dict) else {"estimated": 0.02},
            output=total_costs,
            explanation="Transaction costs including fees, spread, and slippage"
        ))
        
        # Step 6: Net edge
        edge_net = opp["edge_net"]
        breakdown.add_step(CalculationStep(
            name="Net Edge",
            formula="edge_net = raw_edge - costs",
            inputs={"raw_edge": raw_edge, "costs": total_costs},
            output=edge_net,
            explanation="Expected edge after accounting for transaction costs"
        ))
        
        # Step 7: Kelly sizing
        config_snapshot = opp.get("config_snapshot", {})
        lambda_gain = config_snapshot.get("lambda_gain", 0.10)
        
        # Simplified Kelly calculation
        kelly_fraction = edge_net / lambda_gain if lambda_gain > 0 else 0
        size_fraction = min(kelly_fraction, 0.05)  # Cap at 5%
        
        breakdown.add_step(CalculationStep(
            name="Position Sizing",
            formula="size = min(kelly_fraction, max_size)",
            inputs={
                "edge_net": edge_net,
                "lambda_gain": lambda_gain,
                "kelly_fraction": kelly_fraction,
                "max_size": 0.05
            },
            output=size_fraction,
            explanation="Kelly-based position size capped at maximum allocation"
        ))
        
        # Aspects analysis
        breakdown.aspects_analysis = await _analyze_aspects(contributions)
        
        # Risk analysis
        breakdown.risk_analysis = _analyze_risks(opp, market_data)
        
        # Final summary
        breakdown.final_summary = {
            "decision": opp["decision"],
            "confidence": _calculate_confidence(opp, contributions),
            "expected_return": edge_net * size_fraction,
            "max_loss": size_fraction,
            "time_horizon_days": _estimate_time_horizon(market_data),
            "key_risks": _identify_key_risks(opp, contributions)
        }
        
        return breakdown.to_dict()
        
    except Exception as e:
        logger.error(f"Error getting calculation breakdown: {e}")
        raise


async def get_market_calculation_factors(market_id: str) -> Dict[str, Any]:
    """
    Get factors that go into market analysis.
    
    Args:
        market_id: Market ID
        
    Returns:
        Market analysis factors
    """
    try:
        market_data = await _get_market_data(market_id)
        contributions = await _get_aspect_contributions(market_id)
        
        factors = {
            "market_factors": {
                "price_yes": market_data.get("price_yes", 0.5),
                "liquidity_score": market_data.get("liquidity_score", 0.0),
                "rules_clarity": market_data.get("rules_clarity", "medium"),
                "category_tags": market_data.get("category_tags", []),
                "deadline_utc": market_data.get("deadline_utc"),
                "days_until_deadline": _days_until_deadline(market_data.get("deadline_utc"))
            },
            "astrological_factors": {
                "total_aspects": len(contributions),
                "strongest_aspect": _get_strongest_aspect(contributions),
                "aspect_categories": _categorize_aspects(contributions),
                "eclipse_influence": _has_eclipse_influence(contributions),
                "temporal_concentration": _calculate_temporal_concentration(contributions)
            },
            "technical_factors": {
                "volume_indicators": {"status": "not_implemented"},
                "price_momentum": {"status": "not_implemented"},
                "volatility_measures": {"status": "not_implemented"}
            }
        }
        
        return factors
        
    except Exception as e:
        logger.error(f"Error getting market calculation factors: {e}")
        return {}


async def _get_market_data(market_id: str) -> Dict[str, Any]:
    """Get market data."""
    markets = await supabase.select(
        table="markets",
        select="*",
        eq={"id": market_id}
    )
    return markets[0] if markets else {}


async def _get_aspect_contributions(market_id: str) -> List[Dict[str, Any]]:
    """Get aspect contributions for a market."""
    contributions = await supabase.select(
        table="aspect_contributions",
        select="*,aspect_events(*)",
        eq={"market_id": market_id}
    )
    return contributions or []


async def _analyze_aspects(contributions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Analyze aspect contributions."""
    if not contributions:
        return []
    
    analyses = []
    
    for contrib in contributions:
        aspect_event = contrib.get("aspect_events", {})
        
        analysis = {
            "aspect_id": contrib.get("aspect_id"),
            "planets": f"{aspect_event.get('planet1', '?')}-{aspect_event.get('aspect', '?')}-{aspect_event.get('planet2', '?')}",
            "contribution": contrib.get("contribution", 0),
            "peak_date": aspect_event.get("peak_utc"),
            "orb_degrees": aspect_event.get("orb_deg", 0),
            "is_eclipse": aspect_event.get("is_eclipse", False),
            "severity": aspect_event.get("severity", "unknown"),
            "weight_breakdown": {
                "temporal": contrib.get("temporal_w", 0),
                "angular": contrib.get("angular_w", 0),
                "severity": contrib.get("severity_w", 0),
                "category": contrib.get("category_w", 0)
            },
            "interpretation": _interpret_aspect(aspect_event, contrib)
        }
        analyses.append(analysis)
    
    # Sort by absolute contribution
    analyses.sort(key=lambda x: abs(x["contribution"]), reverse=True)
    
    return analyses


def _analyze_risks(opp: Dict[str, Any], market_data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze risks for the opportunity."""
    risks = {
        "market_risks": [],
        "model_risks": [],
        "execution_risks": [],
        "risk_score": 0.0
    }
    
    # Market risks
    if market_data.get("liquidity_score", 0) < 1.0:
        risks["market_risks"].append("Low liquidity may increase slippage")
    
    if market_data.get("rules_clarity") == "ambiguous":
        risks["market_risks"].append("Ambiguous rules increase resolution uncertainty")
    
    # Model risks
    edge_net = opp.get("edge_net", 0)
    if edge_net < 0.05:
        risks["model_risks"].append("Small edge may not be significant")
    
    if abs(opp.get("s_astro", 0)) > 2.0:
        risks["model_risks"].append("Extreme astrology score may indicate model overconfidence")
    
    # Execution risks
    if opp.get("size_fraction", 0) > 0.03:
        risks["execution_risks"].append("Large position size increases impact costs")
    
    # Calculate overall risk score
    risk_factors = len(risks["market_risks"]) + len(risks["model_risks"]) + len(risks["execution_risks"])
    risks["risk_score"] = min(risk_factors * 0.2, 1.0)
    
    return risks


def _calculate_confidence(opp: Dict[str, Any], contributions: List[Dict[str, Any]]) -> float:
    """Calculate confidence score for the opportunity."""
    base_confidence = 0.5
    
    # Edge strength
    edge_net = opp.get("edge_net", 0)
    edge_confidence = min(edge_net * 10, 0.3)  # Max 0.3 from edge
    
    # Number of supporting aspects
    n_contributions = len([c for c in contributions if c.get("contribution", 0) != 0])
    aspect_confidence = min(n_contributions * 0.05, 0.2)  # Max 0.2 from aspects
    
    return min(base_confidence + edge_confidence + aspect_confidence, 1.0)


def _estimate_time_horizon(market_data: Dict[str, Any]) -> int:
    """Estimate time horizon in days."""
    deadline_str = market_data.get("deadline_utc")
    if not deadline_str:
        return 30  # Default
    
    try:
        deadline = datetime.fromisoformat(deadline_str.replace('Z', '+00:00'))
        days_remaining = (deadline - datetime.now(deadline.tzinfo)).days
        return max(1, days_remaining)
    except:
        return 30


def _identify_key_risks(opp: Dict[str, Any], contributions: List[Dict[str, Any]]) -> List[str]:
    """Identify key risks for the opportunity."""
    risks = []
    
    if opp.get("edge_net", 0) < 0.03:
        risks.append("Marginal edge")
    
    if len(contributions) < 2:
        risks.append("Limited astrological support")
    
    if opp.get("size_fraction", 0) > 0.04:
        risks.append("Large position size")
    
    return risks


def _get_strongest_aspect(contributions: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Get the strongest aspect contribution."""
    if not contributions:
        return None
    
    strongest = max(contributions, key=lambda c: abs(c.get("contribution", 0)))
    aspect_event = strongest.get("aspect_events", {})
    
    return {
        "planets": f"{aspect_event.get('planet1', '?')}-{aspect_event.get('aspect', '?')}-{aspect_event.get('planet2', '?')}",
        "contribution": strongest.get("contribution", 0),
        "peak_date": aspect_event.get("peak_utc")
    }


def _categorize_aspects(contributions: List[Dict[str, Any]]) -> Dict[str, int]:
    """Categorize aspects by type."""
    categories = {}
    
    for contrib in contributions:
        aspect_event = contrib.get("aspect_events", {})
        aspect_type = aspect_event.get("aspect", "unknown")
        categories[aspect_type] = categories.get(aspect_type, 0) + 1
    
    return categories


def _has_eclipse_influence(contributions: List[Dict[str, Any]]) -> bool:
    """Check if any contributing aspects are eclipses."""
    return any(
        contrib.get("aspect_events", {}).get("is_eclipse", False) 
        for contrib in contributions
    )


def _calculate_temporal_concentration(contributions: List[Dict[str, Any]]) -> float:
    """Calculate how temporally concentrated the aspects are."""
    if len(contributions) < 2:
        return 1.0
    
    # Get peak dates
    dates = []
    for contrib in contributions:
        peak_utc = contrib.get("aspect_events", {}).get("peak_utc")
        if peak_utc:
            try:
                date = datetime.fromisoformat(peak_utc.replace('Z', '+00:00'))
                dates.append(date)
            except:
                continue
    
    if len(dates) < 2:
        return 1.0
    
    # Calculate spread in days
    dates.sort()
    spread_days = (dates[-1] - dates[0]).days
    
    # More concentrated = higher score
    return max(0.1, min(1.0, 30.0 / max(1, spread_days)))


def _days_until_deadline(deadline_str: Optional[str]) -> Optional[int]:
    """Calculate days until deadline."""
    if not deadline_str:
        return None
    
    try:
        deadline = datetime.fromisoformat(deadline_str.replace('Z', '+00:00'))
        days = (deadline - datetime.now(deadline.tzinfo)).days
        return max(0, days)
    except:
        return None


def _interpret_aspect(aspect_event: Dict[str, Any], contrib: Dict[str, Any]) -> str:
    """Provide interpretation of an aspect's influence."""
    planet1 = aspect_event.get("planet1", "?")
    planet2 = aspect_event.get("planet2", "?")
    aspect_type = aspect_event.get("aspect", "?")
    contribution = contrib.get("contribution", 0)
    
    direction = "bullish" if contribution > 0 else "bearish" if contribution < 0 else "neutral"
    strength = "strong" if abs(contribution) > 0.5 else "moderate" if abs(contribution) > 0.2 else "weak"
    
    base_interpretation = f"{strength.capitalize()} {direction} influence from {planet1}-{aspect_type}-{planet2}"
    
    if aspect_event.get("is_eclipse", False):
        base_interpretation += " (eclipse amplification)"
    
    return base_interpretation


def _logit(p: float) -> float:
    """Calculate logit function."""
    p = max(0.001, min(0.999, p))  # Clamp to avoid infinities
    return math.log(p / (1 - p))