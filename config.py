class config:
    def __init__(self):
        self.env_type='prod'
        if self.env_type=='qa':
            print("Running QA instance")
            from configurations import config_qa as cp
            self.config = cp
        if self.env_type=='prod':
            print("Running Prod instance")
            from configurations import config_prod as cp
            self.config = cp
