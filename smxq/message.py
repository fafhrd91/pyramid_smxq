""" Custom Message class """
from haigha.message import Message
from haigha.classes import BasicClass


class SmxqBasicClass(BasicClass):
    pass


class SmxqMessage(Message):

    @property
    def channel(self):
        return self._delivery_info['channel']

    def ack(self):
        self._delivery_info['channel'].basic.ack(
            self._delivery_info['delivery_tag'])


# monkey patch Message
import haigha.classes.basic_class
haigha.classes.basic_class.Message = SmxqMessage
