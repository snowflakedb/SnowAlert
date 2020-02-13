from . import percentiles

__all__ = ['percentiles']

baselines = {'percentiles': percentiles}

BASELINE_OPTIONS = [
    {
        'baseline': name,
        'options': getattr(baseline, 'OPTIONS'),
        'docstring': baseline.__doc__,
    }
    for name, baseline in baselines.items()
    if (
        getattr(baseline, 'OPTIONS', [{}])[0].get('name')
        and callable(getattr(baseline, 'create', None))
    )
]
