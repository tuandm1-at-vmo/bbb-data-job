'''
Contains a basic class that supports logging and naming.
'''

import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar


V = TypeVar('V')


class PrettyGoodInstance:
    ''' An instance that we considerably think to be good. '''
    _name: str = '?'
    ''' Name of this instance. '''
    _logger: logging.Logger
    ''' A logger associated with this instance. '''

    def __str__(self):
        return self.name()

    def name(self):
        ''' Returns the name of this instance. '''
        return self._name.lower()

    def _enable_logger(self, debug: bool):
        ''' Enables logging for this instance. '''
        class_name = re.match('<class \'(.+)\'>', str(self.__class__))
        exact_class_name = class_name.group(1) if class_name is not None else ''
        self._logger = logging.getLogger(name=f'{exact_class_name}<{self.name()}>')
        if debug: self._logger.setLevel(logging.DEBUG)
        else: self._logger.setLevel(logging.INFO)

    def _self_validate(self):
        '''
        Validates its properties before finishing the initialization.

        It should raise a `RuntimeError` as soon as any validation is broken.
        '''
        pass


class NotImplementedYet(RuntimeError):
    ''' This error should be raised once a function/method wasn't implemented correctly. '''

    def __init__(self, *args: object) -> None:
        super().__init__('This function hasn\'t been implemented yet', *args)


class Entity(dict):
    ''' A wrapper class for type `dict`. '''

    def __init__(self, obj: Dict[str, Any]):
        super().__init__()
        for key in obj.keys():
            self[key] = obj.get(key)

    def get(self, key: str, type: Type[V] = Any) -> Optional[V]:
        if key in self.keys():
            return self[key]

    def table(self) -> str:
        raise NotImplementedYet()

    def columns(self, ignore_cols: List[str] = [], wrap_in_box_brackets = False) -> Tuple[str]:
        cols: List[str] = []
        for key in self.keys():
            col = str(key)
            if col.startswith('__'): continue # ignore private attributes
            if col not in ignore_cols:
                cols.append(f'[{col}]' if wrap_in_box_brackets else col)
        return tuple(cols) # type: ignore

    def values(self, columns: Optional[Tuple[str]] = None, ignore_cols: List[str] = []):
        vals = []
        if columns is None:
            columns = self.columns(ignore_cols=ignore_cols)
        for col in columns:
            val = None
            if col in self.keys():
                val = self[col]
            vals.append(val)
        return tuple(vals)

    def load_from_tuple(self, *args):
        cols = self.columns()
        for i in range(len(cols)):
            if i >= len(args): break
            self[cols[i]] = args[i]
        return self

    def __str__(self):
        this = {}
        for col in self.columns():
            this[col] = self[col]
        return str(this)


class Object(Entity):
    ''' An alias for class `Entity`. '''