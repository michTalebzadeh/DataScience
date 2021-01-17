import yaml
with open("../conf/config.yml", 'r') as file:
  config: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
  #print(config.keys())
  #print(config['hiveVariables'].items())





