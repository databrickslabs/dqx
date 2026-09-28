"""DQProfiler option helpers shared by the profiler routes and the demo seeder."""

from databricks_labs_dqx_app.backend.services.app_settings_service import ProfilerSample


def sample_profile_options(sample: ProfilerSample, requested: dict[str, object] | None) -> dict[str, object]:
    """Merge *sample* into the DQProfiler options for a run.

    The profiler applies its own ``DEFAULT_PROFILE_OPTIONS`` for anything we
    omit — including ``sample_fraction: 0.3`` and ``limit: 1000``. Those
    defaults would silently shrink every profile run to ~300 rows, so we always
    send both keys explicitly and let the view carry the sampling instead.
    ``limit: 0`` and ``sample_fraction: None`` together mean "profile whatever
    the view returns".

    Args:
        sample: The resolved sampling policy. Present for call-site clarity —
            the pinning is the same whichever policy applies, because the view
            already carries it.
        requested: Caller-supplied profiler options. A ``filter`` here is still
            honoured; it runs before sampling.

    Returns:
        The options to send to ``DQProfiler.profile``.
    """
    options: dict[str, object] = dict(requested or {})
    options["sample_fraction"] = None
    options["limit"] = 0
    return options
