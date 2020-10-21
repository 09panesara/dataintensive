import torch
import pandas as pd
from sklearn.model_selection import train_test_split
import numpy as np


def data_split(fpath='../data/graph.csv'):
    df = pd.read_csv(fpath) # only have edges from a->b here
    graph = torch.tensor([df['source'].tolist(), df['target'].tolist()], dtype=torch.long)
    tr, test = train_test_split(np.transpose(graph.numpy()), test_size=0.2, random_state=42)
    train, val = train_test_split(tr, test_size=0.2, random_state=42)
  
    return train, test, val


train, test, val = data_split()
pd.DataFrame(train).to_csv("../data/train.csv", index=False)
pd.DataFrame(test).to_csv("../data/test.csv", index=False)
pd.DataFrame(val).to_csv("../data/val.csv", index=False)

print('Done!')

    