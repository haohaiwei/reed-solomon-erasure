use thiserror::Error;

#[non_exhaustive]
#[derive(Error, PartialEq, Debug, Clone)]
pub enum Error {
    #[error("The number of provided shards is smaller than the one in codec")]
    TooFewShards,
    #[error("The number of provided shards is greater than the one in codec")]
    TooManyShards,
    #[error("The number of provided data shards is smaller than the one in codec")]
    TooFewDataShards,
    #[error("The number of provided data shards is greater than the one in codec")]
    TooManyDataShards,
    #[error("The number of provided parity shards is smaller than the one in codec")]
    TooFewParityShards,
    #[error("The number of provided parity shards is greater than the one in codec")]
    TooManyParityShards,
    #[error("The number of provided buffer shards is smaller than the number of parity shards in codec")]
    TooFewBufferShards,
    #[error("The number of provided buffer shards is greater than the number of parity shards in codec")]
    TooManyBufferShards,
    #[error("At least one of the provided shards is not of the correct size")]
    IncorrectShardSize,
    #[error("The number of shards present is smaller than number of parity shards, cannot reconstruct missing shards")]
    TooFewShardsPresent,
    #[error("The first shard provided is of zero length")]
    EmptyShard,
    #[error("The number of flags does not match the total number of shards")]
    InvalidShardFlags,
    #[error("The data shard index provided is greater or equal to the number of data shards in codec")]
    InvalidIndex,
}

#[non_exhaustive]
#[derive(Error, PartialEq, Debug, Clone)]
pub enum SBSError {
    #[error("Too many calls")]
    TooManyCalls,
    #[error("Leftover shards")]
    LeftoverShards,
    #[error("{0}")]
    RSError(#[source] Error),
}

#[cfg(test)]
mod tests {
    use crate::errors::Error;
    use crate::errors::SBSError;

    #[test]
    fn test_error_to_string_is_okay() {
        assert_eq!(
            Error::TooFewShards.to_string(),
            "The number of provided shards is smaller than the one in codec"
        );
        assert_eq!(
            Error::TooManyShards.to_string(),
            "The number of provided shards is greater than the one in codec"
        );
        assert_eq!(
            Error::TooFewDataShards.to_string(),
            "The number of provided data shards is smaller than the one in codec"
        );
        assert_eq!(
            Error::TooManyDataShards.to_string(),
            "The number of provided data shards is greater than the one in codec"
        );
        assert_eq!(
            Error::TooFewParityShards.to_string(),
            "The number of provided parity shards is smaller than the one in codec"
        );
        assert_eq!(
            Error::TooManyParityShards.to_string(),
            "The number of provided parity shards is greater than the one in codec"
        );
        assert_eq!(
            Error::TooFewBufferShards.to_string(),
            "The number of provided buffer shards is smaller than the number of parity shards in codec"
        );
        assert_eq!(
            Error::TooManyBufferShards.to_string(),
            "The number of provided buffer shards is greater than the number of parity shards in codec"
        );
        assert_eq!(
            Error::IncorrectShardSize.to_string(),
            "At least one of the provided shards is not of the correct size"
        );
        assert_eq!(Error::TooFewShardsPresent.to_string(), "The number of shards present is smaller than number of parity shards, cannot reconstruct missing shards");
        assert_eq!(
            Error::EmptyShard.to_string(),
            "The first shard provided is of zero length"
        );
        assert_eq!(
            Error::InvalidShardFlags.to_string(),
            "The number of flags does not match the total number of shards"
        );
        assert_eq!(
            Error::InvalidIndex.to_string(),
            "The data shard index provided is greater or equal to the number of data shards in codec"
        );
    }

    #[test]
    fn test_sbserror_to_string_is_okay() {
        assert_eq!(SBSError::TooManyCalls.to_string(), "Too many calls");
        assert_eq!(SBSError::LeftoverShards.to_string(), "Leftover shards");
        assert_eq!(
            SBSError::RSError(Error::TooFewShards).to_string(),
            "The number of provided shards is smaller than the one in codec"
        );
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_error_display_does_not_panic() {
        println!("{}", Error::TooFewShards);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_sbserror_display_does_not_panic() {
        println!("{}", SBSError::TooManyCalls);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_sbserror_source_chain() {
        let err = SBSError::RSError(Error::TooFewShards);
        use std::error::Error as _;
        assert!(err.source().is_some());
    }
}
