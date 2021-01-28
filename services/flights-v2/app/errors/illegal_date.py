class IllegalDateException(Exception):
    def __init__(self, date):
        super().__init__("Date not legal: {}".format(date))
        self.errors = "Illegal Date Exception"
        print(self.errors)
