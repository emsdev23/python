import yaml
import socket

class Config:
      with open('config.yaml','r') as file:
         config = yaml.safe_load(file)