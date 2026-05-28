"""
Sentiment Enrichment Module
============================
Uses VADER (rule-based, optimised for social media) + TextBlob (ML-based)
for ensemble sentiment scoring.

100% local — zero API calls, zero cost, <5ms latency per document.

Sentiment labels: POSITIVE | NEGATIVE | NEUTRAL | MIXED
"""

from dataclasses import dataclass

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

# ─── Data Classes ─────────────────────────────────────────────────────────[...]


@dataclass
class SentimentResult:
    """Result of sentiment analysis."""

    label:         str          # POSITIVE | NEGATIVE | NEUTRAL | MIXED
    compound:      float        # -1.0 to 1.0
    vader_pos:     float
    vader_neg:     float
    vader_neu:     float
    textblob_pol:  float        # -1.0 to 1.0
    textblob_sub:  float        # 0.0 to 1.0
    confidence:    float        # agreement between models (0-1)


# ─── Analyser ──────────────────────────────────────────────────────────────[...]


class SentimentEnricher:
    """Sentiment analysis using VADER and TextBlob ensemble."""

    def __init__(self):
        """Initialize sentiment analyzer."""
        self._vader = SentimentIntensityAnalyzer()

    def analyse(self, text: str) -> SentimentResult:
        """Analyse text and return sentiment result."""
        if not text or not text.strip():
            return SentimentResult(
                label="NEUTRAL", compound=0.0,
                vader_pos=0.0, vader_neg=0.0, vader_neu=1.0,
                textblob_pol=0.0, textblob_sub=0.0, confidence=1.0,
            )

        # --- VADER ---
        v       = self._vader.polarity_scores(text)
        v_comp  = v["compound"]

        # --- TextBlob ---
        blob    = TextBlob(text)
        tb_pol  = blob.sentiment.polarity
        tb_sub  = blob.sentiment.subjectivity

        # --- Ensemble compound (weighted average) ---
        compound = round(0.6 * v_comp + 0.4 * tb_pol, 4)

        # --- Label ---
        label = self._label(compound, v_comp, tb_pol)

        # --- Confidence: do both models agree on direction? ---
        agree     = (v_comp >= 0) == (tb_pol >= 0)
        magnitude = (abs(v_comp) + abs(tb_pol)) / 2
        confidence = round(magnitude if agree else magnitude * 0.5, 4)

        return SentimentResult(
            label        = label,
            compound     = compound,
            vader_pos    = round(v["pos"], 4),
            vader_neg    = round(v["neg"], 4),
            vader_neu    = round(v["neu"], 4),
            textblob_pol = round(tb_pol, 4),
            textblob_sub = round(tb_sub, 4),
            confidence   = confidence,
        )

    def analyse_record(self, record: dict) -> dict:
        """Enrich a pipeline record dict in-place and return it."""
        text  = f"{record.get('title', '')} {record.get('text', '')}".strip()
        result = self.analyse(text)
        record.update({
            "sentiment_label":     result.label,
            "sentiment_compound":  result.compound,
            "vader_pos":           result.vader_pos,
            "vader_neg":           result.vader_neg,
            "vader_neu":           result.vader_neu,
            "textblob_polarity":   result.textblob_pol,
            "textblob_subjectivity": result.textblob_sub,
            "sentiment_confidence": result.confidence,
        })
        return record

    @staticmethod
    def _label(compound: float, v_comp: float, tb_pol: float) -> str:
        """Determine sentiment label based on compound and component scores."""
        same_sign = (v_comp >= 0) == (tb_pol >= 0)
        if not same_sign and abs(v_comp) > 0.2 and abs(tb_pol) > 0.2:
            return "MIXED"
        if compound >= 0.05:
            return "POSITIVE"
        if compound <= -0.05:
            return "NEGATIVE"
        return "NEUTRAL"


# ─── Quick test ─────────────────────────────────────────────────────────────[...]

if __name__ == "__main__":
    e = SentimentEnricher()
    samples = [
        "This is an amazing breakthrough in machine learning!",
        "The API is broken and keeps throwing errors. Terrible experience.",
        "Updated the README with installation instructions.",
        "Excited about the new features but worried about the performance hit.",
    ]
    for s in samples:
        r = e.analyse(s)
        print(f"[{r.label:8s}] compound={r.compound:+.3f} conf={r.confidence:.2f} | {s[:60]}")
