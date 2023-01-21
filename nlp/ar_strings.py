from camel_tools.utils.charsets import AR_LETTERS_CHARSET

AR_STR = r'[' + u''.join(AR_LETTERS_CHARSET) + ']+'
WORD = r'(?:\s' + AR_STR + ')'
