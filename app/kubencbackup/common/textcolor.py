class TextColor:
    __allowed_colors = {
        'GREEN':    "\033[0;32m",
        'YELLOW':   "\033[0;33m",
        'RED':      "\033[0;31m",
        'NC':       "\033[0m"
    }

    def GREEN():
        return TextColor.__allowed_colors['GREEN']

    def YELLOW():
        return TextColor.__allowed_colors['YELLOW']

    def RED():
        return TextColor.__allowed_colors['RED']

    def NO_COL():
        return TextColor.__allowed_colors['NC']

    def wrap_text(col, text):
        if col in TextColor.__allowed_colors.values():
            return col + text + TextColor.NO_COL()
        else:
            return text

    def wrapped_text_overhead(col):
        return len(col) + len(TextColor.NO_COL())
