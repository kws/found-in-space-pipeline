CANONICAL_EPOCH_JYEAR = 2016.0  # Gaia DR3 reference epoch (J2016.0)

EPS = 1e-12  # Epsilon for numerical stability

# σ_M ≈ 2.17 * (σ_r / r) for distance-modulus uncertainty in mag
PHOTOMETRY_QUALITY_DM_FACTOR = 2.17

# Teff bounds for blackbody RGB (Tanner Helland algorithm range)
TEFF_MIN_K = 1000
TEFF_MAX_K = 40000

# Default when no color/Teff available (solar-type)
TEFF_DEFAULT_K = 5800


OUTPUT_COLS = [
    "morton_code",
    "source",
    "source_id",
    "x_icrs_pc",
    "y_icrs_pc",
    "z_icrs_pc",
    "mag_abs",
    "teff",
    "astrometry_method",
    "astrometry_quality",
    "photometry_method",
    "photometry_quality",
]
