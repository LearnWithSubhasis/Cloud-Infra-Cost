import json

dict_lookup = {
    # "obfuscate_from_text": "obfuscate_to_text",


}

"""
Dictionary lookup, using key
"""
def o(input:str) -> str:
    return input in dict_lookup.keys() and dict_lookup[input] or input

"""
Obfuscate whole json
"""
def o2(input_dict:dict) -> str:
    input_str = json.dumps(input_dict)
    for key_dict in dict_lookup.keys():
        if input_str.find(key_dict) >= 0:
            input_str = input_str.replace(key_dict, dict_lookup[key_dict])
    input_dict = json.loads(input_str)
    return input_dict

"""
Reverse lookup, using value
"""
def o3(input: str) -> str:
    for key_dict in dict_lookup.keys():
        if input == dict_lookup[key_dict]:
            return key_dict
    return input

