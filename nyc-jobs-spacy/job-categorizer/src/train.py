from spacy.training import Example
import spacy
from dataset import JobDataset

def train_model(config_path, output_dir):
    nlp = spacy.blank("en")  # Create a blank English model
    config = spacy.util.load_config(config_path)

    # Load the dataset
    dataset = JobDataset()
    train_data = dataset.load_data()

    # Create the pipeline
    for name, component in config["components"].items():
        nlp.add_pipe(name, config=component)

    # Training loop
    for epoch in range(config["training"]["epochs"]):
        losses = {}
        for batch in spacy.util.minibatch(train_data, size=config["training"]["batch_size"]):
            examples = []
            for text, annotations in batch:
                doc = nlp.make_doc(text)
                example = Example.from_dict(doc, annotations)
                examples.append(example)
            nlp.update(examples, drop=config["training"]["dropout"], losses=losses)
        print(f"Epoch {epoch + 1}/{config['training']['epochs']}, Losses: {losses}")

    # Save the trained model
    nlp.to_disk(output_dir)

if __name__ == "__main__":
    config_path = "config/config.cfg"
    output_dir = "data/processed/model"
    train_model(config_path, output_dir)