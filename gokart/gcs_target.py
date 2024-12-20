from urllib.parse import urlsplit

import luigi
import luigi.contrib.gcs


class GCSWithMetaDataTarget(luigi.contrib.gcs.GCSTarget):
    def set_metadata(self, metadata: dict[str, str]):
        bucket_name, blob_name = self._get_bucket_and_blob_name(self.path)
        blob = self.fs.bucket(bucket_name).blob(blob_name)
        metageneration_match_precondition = blob.metageneration
        blob.metadata = metadata
        blob.patch(if_metageneration_match=metageneration_match_precondition)

    @staticmethod
    def _get_bucket_and_blob_name(path: str) -> tuple[str, str]:
        (_, bucket_name, path, _, _) = urlsplit(path)
        blob_name = path[1:]
        return bucket_name, blob_name
