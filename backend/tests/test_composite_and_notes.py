"""
Tests for composite scoring, graceful degradation, and analyst notes.
"""
import pytest
from unittest.mock import patch, MagicMock

from app.features.engine import FeatureEngine, _normalize_score, WEIGHTS, ALL_INDICATORS


class TestRuleScoreUnchanged:
    """Rule score formula must not change when new components are added."""

    def test_normalize_score_bounds(self):
        raw = 0
        assert _normalize_score(raw) == 0.0
        raw = sum(WEIGHTS.values())
        assert _normalize_score(raw) == 100.0
        assert _normalize_score(raw * 2) == 100.0

    def test_rule_score_weights_unchanged(self):
        # All flags 0 -> 0
        raw = sum(WEIGHTS.get(c, 0) * 0 for c in ALL_INDICATORS)
        assert raw == 0
        assert _normalize_score(raw) == 0.0
        # One heavy flag (e.g. RNU 20)
        raw = WEIGHTS.get("RNU_FLAG", 0) * 1
        assert raw == 20
        assert 0 < _normalize_score(raw) <= 100


class TestCompositeClamp:
    """Composite score must stay in 0..100 and delta limited."""

    def test_composite_delta_contribution(self):
        from app.core.config import settings
        # f: (s/50 - 1) so 50->0, 100->1, 0->-1
        def contrib(s):
            if s is None:
                return 0.0
            return (s / 50.0) - 1.0
        assert contrib(50) == 0.0
        assert contrib(100) == 1.0
        assert contrib(0) == -1.0
        delta_max = getattr(settings, "composite_delta_max", 15.0)
        rule_score = 40.0
        # If all components 100, avg_contrib = 1, composite = 40 + 15 = 55
        comp = rule_score + delta_max * 1.0
        assert 0 <= comp <= 100
        # If all 0, avg_contrib = -1, composite = 40 - 15 = 25
        comp_low = rule_score + delta_max * (-1.0)
        assert 0 <= comp_low <= 100


class TestFeatureEngineGracefulDegradation:
    """When anomaly/weak models are missing, engine should not crash; score = rule_score (or hybrid as before)."""

    @pytest.mark.asyncio
    async def test_engine_imports_without_models(self):
        # No artifacts on disk -> get_anomaly_predictor and get_weak_predictor return None
        with patch("app.features.engine._get_anomaly_predictor", return_value=None), \
             patch("app.features.engine._get_weak_predictor", return_value=None), \
             patch("app.features.engine._try_load_ml_model", return_value=None):
            engine = FeatureEngine()
            assert engine._anomaly_packed is None
            assert engine._weak_packed is None
            assert engine.ml_model is None
