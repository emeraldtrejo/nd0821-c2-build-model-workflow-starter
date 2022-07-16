#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights and Biases parameters [parameter1,parameter2]: parameter1,parameter2,parameter3
"""
import argparse
import logging
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info(f"Downloading artifact {args.input_artifact}")

    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    logger.info(f"cleaning artifact with min & max prices {args.min_price, args.max_price}")
    
    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info(f"Saving cleaned data to {args.output_artifact}")
    # save cleaned data
    df.to_csv(args.output_artifact, index=False)

    logger.info(f"Logging artifact {args.output_artifact}") 
    # log artifact to W&B
    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)

    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Raw dataset to be cleaned",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="description of the output (e.g data with outliers and null values removed)",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="the minimum price to consider and filter the price data for (e.g 50)",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="the maximum price to consider and filter the price data for (e.g 150)",
        required=True
    )

    args = parser.parse_args()

    args = parser.parse_args()

    go(args)
