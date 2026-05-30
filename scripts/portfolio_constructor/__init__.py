"""portfolio_constructor — Stage-2 greedy portfolio construction.

Heavy modules (data, edge, validate, constructor, cli) are loaded lazily;
this __init__ does not import them so the package can coexist in the CI
Python step without numpy/lightgbm being present.  The CI test imports
overlap/selector/sizing directly via sys.path, bypassing this file.
"""
# Lazy imports: only materialise when called from local .venv-analysis env.

def _lazy_import():
    from .constructor import PortfolioConfig, PortfolioResult, run_constructor
    return PortfolioConfig, PortfolioResult, run_constructor


def __getattr__(name):
    names = {'PortfolioConfig', 'PortfolioResult', 'run_constructor'}
    if name in names:
        cfg, res, run = _lazy_import()
        globals()['PortfolioConfig'] = cfg
        globals()['PortfolioResult'] = res
        globals()['run_constructor'] = run
        return globals()[name]
    raise AttributeError(f"module 'portfolio_constructor' has no attribute {name!r}")


__all__ = ['PortfolioConfig', 'PortfolioResult', 'run_constructor']
