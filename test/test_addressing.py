"""
Test the actor system

@author aevans
"""
from actors.address.addressing import ActorAddress, get_address
import re


def test_get_address():
    addr = get_address('127.0.0.1', 12000)
    assert(addr.host == '127.0.0.1')
    assert(addr.port == 12000)
    assert('127.0.0.1' in addr.address)
    assert(re.match('[0-9\.]+\_\d+\_\d+', addr.address) is not None)
