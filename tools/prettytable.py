# coding: utf-8

class PrettyTable(object):
    """Print table like database shell."""

    def __init__(self, field_names=None):
        super(PrettyTable, self).__init__()
        self.field_names = field_names
        self.rows = []

    def __str__(self):
        return self.to_string()

    def add_row(self, row):
        if self.field_names and len(row) != len(self.field_names):
            raise Exception("Row has invalid number of values", row)
        self.rows.append(list(row))

    def to_string(self):
        if self.field_names:
            width = map(len, self.field_names)
        else:
            width = [0] * len(self.rows[0])
        for row in self.rows:
            for i, e in enumerate(row):
                width[i] = max(width[i], len(str(e)))

        lines = []
        sep_line = _join(['-' * (w + 2) for w in width])
        lines.append(sep_line)
        if self.field_names:
            lines.append(_join([str(e).rjust(width[i] + 1) + " "
                                for i, e in enumerate(self.field_names)], '|'))
            lines.append(sep_line)
        for row in self.rows:
            lines.append(_join([str(e).rjust(width[i] + 1) + " "
                                for i, e in enumerate(row)], '|'))
        lines.append(sep_line)
        return "\n".join(lines)

def _join(lst, sep='+'):
    lst = [""] + lst + [""]
    return sep.join(lst)
