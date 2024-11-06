# instructions.py
from abc import ABC, abstractmethod
from reflector.etl import RtTemplateVariable
from reflector.events import DataEventMessageBoard, DataEvent, DataEventType
from rt_core_v2.rttuple import RtTuple
from rt_core_v2.ids_codes.rui import Rui

class RtAbstractInstruction(ABC): 
    @abstractmethod
    def execute(field_sys_vars: list[str], variables: dict[str, RtTemplateVariable]) -> bool:
        pass

class RtVariableAssignmentInstruction(RtAbstractInstruction):
    def __init__(self, var_name: str):
        self.var_name = var_name
        
    @abstractmethod
    def getVariable() -> RtTemplateVariable:
        pass

class RtAnnotationInstruction(RtAbstractInstruction):
    def __init__(self, event_type: DataEventType, field_name: str, field_order_in_table: int):
        self.event_type = event_type
        self.field_name = field_name
        self.field_order_in_table = field_order_in_table

    def execute(self, args: list[str], variables: dict[str, RtTemplateVariable]) -> bool:
        rec_num_var = variables["RECORD_NUMBER"]
        record_number = rec_num_var.value if rec_num_var else 0 

        field_value = args[self.field_order_in_table]
        data_event = DataEvent(self.field_name, field_value, self.event_type, record_number) #TODO - need to send through the field number and record number somehow (copied TODO from java code)
        DataEventMessageBoard.publish(data_event)

        return True

#TODO Fix this class, as it is close to a direct translation. Class is 80% close to being done
# class RtTupleCompletionInstruction(RtAbstractInstruction):
#     def __init__(self, tuple_block_fields: list[str], content_block_fields: list[str], subfield_delim: str, quote_open: str, quote_close: str):
#         self.tuple_block_fields = tuple_block_fields.copy()
#         self.content_block_fields = content_block_fields.copy()
#         #TODO Figure out how to implement the factory
#         self.tFactory = RtTupleFactory()
#         self.subfield_delim = subfield_delim
#         self.quote_open = quote_open
#         self.quote_close = quote_close
#         self.tuple = None

#     def execute(self, args: list[str], variables: dict[str, RtTemplateVariable]) -> bool:
#         tuple_block = self._process_fields(self.tuple_block_fields, variables)
#         content_block = self._process_fields(self.content_block_fields, variables, args)

#         # Build the tuple
#         #TODO Same as previous factory TODO
#         self.tuple = self.tFactory.build_rt_tuple_or_temporal_region(tuple_block, content_block)
#         return self.tuple is not None

#     def _process_fields(self, fields: list[str], variables: dict[str, RtTemplateVariable], args: list[str] = None) -> list[str]:
#         processed_fields = []
#         for s in fields:
#             if s.startswith("[") and s.endswith("]"):
#                 command = s[1:-1]
#                 if command == "new-rui":
#                     processed_fields.append(str(Rui()))
#                 elif command in variables:
#                     processed_fields.append(variables[command].value)
#                 else:
#                     print(f"Unknown command or variable: {command}")
#             elif self.subfield_delim in s:
#                 processed_fields.append(self._handle_subfields(s, variables))
#             elif s.startswith("%") and args:
#                 field_num = int(s[1:])
#                 if field_num < len(args):
#                     processed_fields.append(args[field_num])
#             elif "=" in s:
#                 processed_fields.append(self._handle_assignment(s, variables))
#             else:
#                 processed_fields.append(s)
#         return processed_fields

#     def _handle_subfields(self, s: str, variables: dict[str, RtTemplateVariable]) -> str:
#         substitution = []
#         subfields = s.split(self.subfield_delim)
#         for sub in subfields:
#             ref_info = sub.split("=")
#             if len(ref_info) > 1:
#                 command = ref_info[1].strip("[] ")
#                 var_value = variables[command].value if command in variables else "null"
#                 substitution.append(f"{ref_info[0].strip()}={var_value}")
#         return self.subfield_delim.join(substitution)

#     def _handle_assignment(self, s: str, variables: dict[str, RtTemplateVariable]) -> str:
#         ref_info = s.split("=")
#         if len(ref_info) == 2:
#             command = ref_info[1].strip("[] ")
#             var_value = variables.get(command).value if command in variables else "null"
#             return f"{ref_info[0].strip()}={var_value}"
#         return s

#     def get_tuple(self) -> RtTuple:
#         if self.tuple is None:
#             raise ValueError("Must execute this instruction before getting the RtTuple.")
#         return self.tuple

#     def get_particular_reference(self) -> str:
#         return self.content_block_fields[0]
    
# class RtAssignFieldValueInstruction(RtAbstractInstruction):  

# class RtAssignRuiInstruction(RtAbstractInstruction):  
