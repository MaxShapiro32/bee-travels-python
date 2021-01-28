class TagNotFounException(Exception):
    def __init__(self, tag):
        super().__init__('Tag "{}" does not exist'.format(tag))
        self.errors = "Tag Not Found Exception"
        print(self.errors)
