class DatabaseNotFoundException(Exception):
    def __init__(self, db):
        super().__init__('Invalid database type: "{}"'.format(db))
        self.errors = "Database Not Found Exception"
        print(self.errors)
