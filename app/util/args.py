import argparse
from typing import Any, Dict, Type, TypeVar

from ..core.common import Object


T = TypeVar('T', bound=Any)


def parse(arguments: Dict[str, Type[T]] = {}):
    ''' Parses arguments from command-line execution. '''
    parser = argparse.ArgumentParser()
    for argument_name in arguments.keys():
        if argument_name is None or len(argument_name) <= 0: continue
        argument_flag = '--' + argument_name.strip() \
            .replace('_', '-') \
            .lower()
        argument_type: T = arguments.get(argument_name) or Any
        parser.add_argument(argument_flag, type=argument_type)
    args = parser.parse_args()
    return Object(args.__dict__)