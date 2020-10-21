import argparse
import torch
from torch_geometric.nn import Node2Vec
import pandas as pd


def save_embedding(model, save_dir, suffix):
    torch.save(model.embedding.weight.data.cpu(), f'{save_dir}/embedding_{suffix}.pt')

def load_graph(fpath='../data/graph.csv'):
    df = pd.read_csv(fpath) # only have edges from a->b here
    df_reverse =  df.rename(columns={"source": "target", "target": "source"})# get edges from b->a since node2vec considers an edge directional
    graph = pd.concat([df, df_reverse])
    graph = torch.tensor([graph['source'].tolist(), graph['target'].tolist()], dtype=torch.long)
    return graph

def train(params):
    device = f'cuda:{params.device}' if torch.cuda.is_available() else 'cpu'
    device = torch.device(device)

    data = load_graph(params.dataset)
    model = Node2Vec(data, params.embedding_dim, params.walk_length,
                     params.context_size, params.walks_per_node,
                     sparse=True).to(device)

    loader = model.loader(batch_size=params.batch_size, shuffle=True,
                          num_workers=4)
    optimizer = torch.optim.SparseAdam(model.parameters(), lr=params.lr)

    model.train()
    for epoch in range(1, params.epochs + 1):
        for i, (pos_rw, neg_rw) in enumerate(loader):
            optimizer.zero_grad()
            loss = model.loss(pos_rw.to(device), neg_rw.to(device))
            loss.backward()
            optimizer.step()

            if (i + 1) % params.log_steps == 0:
                print(f'Epoch: {epoch:02d}, Step: {i + 1:03d}/{len(loader)}, '
                      f'Loss: {loss:.4f}')

            if (i + 1) % 100 == 0:  # Save model every 100 steps.
                save_embedding(model, params.model_dir, str(i+1))
        save_embedding(model, params.model_dir, 'final')

def test():
    raise NotImplementedError

def main():
    parser = argparse.ArgumentParser(description='OGBL-DDI (Node2Vec)')
    parser.add_argument('--dataset', type=str, default='../data/graph.csv')
    # TODO change to datadir, ../data/graph and then train should use args.datadir/train.csv, test args.datadir/test.csv etc
    parser.add_argument('--model_dir', type=str, default='../model/node2vec', help='Directory to save model checkpoint in')
    parser.add_argument('--device', type=int, default=0)
    parser.add_argument('--embedding_dim', type=int, default=128)
    parser.add_argument('--walk_length', type=int, default=40)
    parser.add_argument('--context_size', type=int, default=20)
    parser.add_argument('--walks_per_node', type=int, default=10)
    parser.add_argument('--batch_size', type=int, default=256)
    parser.add_argument('--lr', type=float, default=0.01)
    parser.add_argument('--epochs', type=int, default=100)
    parser.add_argument('--log_steps', type=int, default=1)
    args = parser.parse_args()

    train(args)




if __name__ == "__main__":
    main()