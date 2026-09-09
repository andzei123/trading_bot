import copy
from backtest.execution.r2_startup_authority import R2StartupEligibilityIssuer

def test_capability_is_one_use_and_object_identity_bound():
    i=R2StartupEligibilityIssuer(); ctx={'x':1}; h=i.issue(startup_epoch_id='e',context=ctx)
    h2=copy.copy(h)
    assert not i.consume(h2,startup_epoch_id='e',context=ctx)
    h=i.issue(startup_epoch_id='e',context=ctx)
    assert i.consume(h,startup_epoch_id='e',context=ctx)
    assert not i.consume(h,startup_epoch_id='e',context=ctx)

def test_wrong_context_consumes_fail_closed():
    i=R2StartupEligibilityIssuer(); h=i.issue(startup_epoch_id='e',context={'x':1})
    assert not i.consume(h,startup_epoch_id='e',context={'x':2})
    assert not i.consume(h,startup_epoch_id='e',context={'x':1})
