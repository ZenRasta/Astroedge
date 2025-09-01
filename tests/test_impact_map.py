"""Tests for impact map functionality."""

import pytest
from backend.services.impact_map_service import (
    to_keyed_map, explode_rules, weight_for, parse_key, validate_weight_map
)
from backend.schemas import ImpactMapPost


class TestKeyParsing:
    """Test key parsing and validation."""
    
    def test_parse_valid_key(self):
        """Test parsing valid key format."""
        p1, p2, aspect = parse_key("(MARS,SATURN)|square")
        assert (p1, p2) == ("MARS", "SATURN")  # Already in canonical order
        assert aspect == "square"
    
    def test_parse_key_canonical_order(self):
        """Test that planets are returned in canonical order."""
        p1, p2, aspect = parse_key("(SATURN,MARS)|square")
        assert (p1, p2) == ("MARS", "SATURN")  # Canonicalized
        assert aspect == "square"
    
    def test_parse_invalid_key_format(self):
        """Test parsing invalid key formats."""
        with pytest.raises(ValueError, match="Bad key format"):
            parse_key("MARS,SATURN|square")  # Missing parentheses
        
        with pytest.raises(ValueError, match="Bad key format"):
            parse_key("(MARS,SATURN)square")  # Missing pipe
        
        with pytest.raises(ValueError, match="Bad key format"):
            parse_key("(MARS)|square")  # Only one planet
    
    def test_parse_invalid_planet(self):
        """Test parsing with invalid planet names."""
        with pytest.raises(ValueError, match="Invalid planet in key"):
            parse_key("(MARS,INVALID)|square")
    
    def test_parse_invalid_aspect(self):
        """Test parsing with invalid aspect names."""
        with pytest.raises(ValueError, match="Invalid aspect in key"):
            parse_key("(MARS,SATURN)|invalid")


class TestWeightValidation:
    """Test weight map validation."""
    
    def test_valid_weights(self):
        """Test validation of valid weights."""
        weights = {"conflict": 3, "legal_regulatory": -2, "markets_finance": 0}
        clean = validate_weight_map(weights)
        # Zero weights should be filtered out
        assert clean == {"conflict": 3, "legal_regulatory": -2}
    
    def test_weight_out_of_bounds(self):
        """Test validation rejects out-of-bounds weights."""
        with pytest.raises(ValueError, match="out of bounds"):
            validate_weight_map({"conflict": 4})
        
        with pytest.raises(ValueError, match="out of bounds"):
            validate_weight_map({"conflict": -4})
    
    def test_invalid_category(self):
        """Test validation rejects invalid categories."""
        with pytest.raises(ValueError, match="Unknown category"):
            validate_weight_map({"invalid_category": 2})
    
    def test_non_integer_weight(self):
        """Test validation rejects non-integer weights."""
        with pytest.raises(ValueError, match="must be integer"):
            validate_weight_map({"conflict": 2.5})


class TestPayloadConversion:
    """Test payload conversion to keyed format."""
    
    def test_keyed_format_passthrough(self):
        """Test that keyed format is validated and passed through."""
        payload = ImpactMapPost(
            activate=True,
            map_by_key={
                "(MARS,SATURN)|square": {"conflict": 3, "legal_regulatory": 2}
            }
        )
        keyed = to_keyed_map(payload)
        assert "(MARS,SATURN)|square" in keyed
        assert keyed["(MARS,SATURN)|square"] == {"conflict": 3, "legal_regulatory": 2}
    
    def test_nested_to_keyed_conversion(self):
        """Test conversion from nested format to keyed format."""
        payload = ImpactMapPost(
            activate=False,
            map_nested=[
                {
                    "planets": ["SATURN", "MARS"], 
                    "aspect": "square",
                    "weights": {"conflict": 3, "legal_regulatory": 2}
                }
            ]
        )
        keyed = to_keyed_map(payload)
        # Should be canonicalized to (MARS,SATURN)
        assert list(keyed.keys()) == ["(MARS,SATURN)|square"]
        assert keyed["(MARS,SATURN)|square"] == {"conflict": 3, "legal_regulatory": 2}
    
    def test_reject_out_of_bounds_weight(self):
        """Test that out-of-bounds weights are rejected."""
        payload = ImpactMapPost(
            activate=True,
            map_by_key={"(MARS,SATURN)|square": {"conflict": 4}}  # Invalid (4)
        )
        with pytest.raises(ValueError):
            to_keyed_map(payload)
    
    def test_category_validation_error(self):
        """Test that invalid categories are rejected."""
        payload = ImpactMapPost(
            activate=True,
            map_by_key={"(MARS,SATURN)|square": {"conflict": 3, "invalid_cat": 1}}
        )
        with pytest.raises(ValueError):
            to_keyed_map(payload)


class TestRuleExplosion:
    """Test rule explosion and row counting."""
    
    def test_zero_weights_skipped_and_rowcount(self):
        """Test that zero weights are skipped and row count is correct."""
        payload = ImpactMapPost(
            activate=True,
            map_by_key={
                "(MARS,SATURN)|square": {
                    "conflict": 3, 
                    "accidents_infrastructure": 0,  # Should be skipped
                    "legal_regulatory": 2
                }
            }
        )
        keyed = to_keyed_map(payload)
        
        # Only non-zeros should remain
        assert set(keyed["(MARS,SATURN)|square"].keys()) == {"conflict", "legal_regulatory"}
        
        # Explode to rules
        rows = explode_rules("test-version-id", keyed)
        assert len(rows) == 2  # Equals sum of non-zero entries
        
        # Check rule contents
        rule_data = [(r.category, r.weight) for r in rows]
        assert ("conflict", 3) in rule_data
        assert ("legal_regulatory", 2) in rule_data
    
    def test_multiple_aspects_explosion(self):
        """Test explosion of multiple aspects."""
        keyed_map = {
            "(MARS,SATURN)|square": {"conflict": 3, "legal_regulatory": 2},
            "(MARS,URANUS)|opposition": {"conflict": 2, "accidents_infrastructure": 3}
        }
        
        rows = explode_rules("test-version", keyed_map)
        assert len(rows) == 4  # 2 + 2 entries
        
        # Check that we have the right planet pairs and aspects
        mars_saturn_square = [r for r in rows if r.planet1 == "MARS" and r.planet2 == "SATURN" and r.aspect == "square"]
        mars_uranus_opp = [r for r in rows if r.planet1 == "MARS" and r.planet2 == "URANUS" and r.aspect == "opposition"]
        
        assert len(mars_saturn_square) == 2
        assert len(mars_uranus_opp) == 2


class TestNestedEquivalence:
    """Test equivalence between nested and keyed formats."""
    
    def test_nested_to_keyed_equivalence(self):
        """Test that nested format converts correctly to keyed format."""
        payload_nested = ImpactMapPost(
            activate=False,
            map_nested=[
                {
                    "planets": ["SATURN", "MARS"], 
                    "aspect": "square",
                    "weights": {"conflict": 3, "legal_regulatory": 2}
                }
            ]
        )
        
        keyed = to_keyed_map(payload_nested)
        assert list(keyed.keys()) == ["(MARS,SATURN)|square"]
        assert keyed["(MARS,SATURN)|square"] == {"conflict": 3, "legal_regulatory": 2}


class TestWeightForFunction:
    """Test the weight_for helper function."""
    
    @pytest.mark.integration
    def test_weight_for_active_version(self, monkeypatch):
        """Test weight_for function with mocked active version."""
        from backend.services import impact_map_service as svc
        
        def fake_active():
            return {"version_id": "vid1", "created_at": "2024-01-01T00:00:00Z", "map": {}}
        
        def fake_fetch(vid, p1, p2, asp, tags):
            assert vid == "vid1"
            assert p1 == "MARS" and p2 == "SATURN" and asp == "square"
            # Return mock rules: conflict=3, legal_regulatory=2
            return [{"weight": 3}, {"weight": 2}]
        
        monkeypatch.setattr(svc, "get_active_map_version_with_json", fake_active)
        monkeypatch.setattr(svc, "fetch_rules_for_version", fake_fetch)
        
        # Test canonical ordering - should work regardless of input order
        weight = svc.weight_for(("SATURN", "MARS"), "square", ["conflict", "legal_regulatory"])
        assert weight == 5.0
    
    @pytest.mark.integration 
    def test_weight_for_no_active_version(self, monkeypatch):
        """Test weight_for returns 0 when no active version exists."""
        from backend.services import impact_map_service as svc
        
        def fake_no_active():
            return {"version_id": None, "created_at": None, "map": {}}
        
        monkeypatch.setattr(svc, "get_active_map_version_with_json", fake_no_active)
        
        weight = svc.weight_for(("MARS", "SATURN"), "square", ["conflict"])
        assert weight == 0.0
    
    @pytest.mark.integration
    def test_weight_for_no_matching_rules(self, monkeypatch):
        """Test weight_for returns 0 when no rules match."""
        from backend.services import impact_map_service as svc
        
        def fake_active():
            return {"version_id": "vid1", "created_at": "2024-01-01T00:00:00Z", "map": {}}
        
        def fake_fetch_empty(vid, p1, p2, asp, tags):
            return []  # No matching rules
        
        monkeypatch.setattr(svc, "get_active_map_version_with_json", fake_active)
        monkeypatch.setattr(svc, "fetch_rules_for_version", fake_fetch_empty)
        
        weight = svc.weight_for(("MARS", "SATURN"), "square", ["conflict"])
        assert weight == 0.0


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_map_rejected(self):
        """Test that empty maps are rejected."""
        from backend.services.impact_map_service import create_new_impact_map
        
        payload = ImpactMapPost(
            activate=True,
            map_by_key={"(MARS,SATURN)|square": {}}  # All weights are zero/missing
        )
        
        with pytest.raises(ValueError, match="no non-zero weights"):
            create_new_impact_map(payload)
    
    def test_all_zero_weights_rejected(self):
        """Test that maps with only zero weights are rejected."""
        from backend.services.impact_map_service import create_new_impact_map
        
        payload = ImpactMapPost(
            activate=True,
            map_by_key={"(MARS,SATURN)|square": {"conflict": 0, "legal_regulatory": 0}}
        )
        
        with pytest.raises(ValueError, match="no non-zero weights"):
            create_new_impact_map(payload)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])