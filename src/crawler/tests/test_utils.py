import utils

def test__chunks():
    chunks = utils.chunks([1,2,3], 3)
    assert len(list(chunks)) == 1
    chunks = utils.chunks([1,2,3,4], 3)
    assert len(list(chunks)) == 2
