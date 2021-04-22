"""Functions that write chunked gzipped files."""
import io
import logging
import gzip

LOGGER = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE_MB = 1000
DEFAULT_MAX_CHUNKS = 20

# Detecting compressed file size at write time is not possible by GzipFile.
# The data hase to be written into the file first before the actual compression performed.
# We need to use a good estimate for a text gzip file to split the file at write time.
EST_COMPR_RATE = 0.12


# pylint: disable=W0622
def open(base_filename, mode='wb', chunk_size_mb=None, max_chunks=None, est_compr_rate=None):
    """Open a gzip-compressed file in binary or text mode.

    Args:
        base_filename: Path where to create the zip file(s) with the exported data.
                       Dynamic chunk numbers are appended to the base_filename
        mode: "wb" or "wt". (Default: wb)
        chunk_size_mb: File chunk sizes. (Default: 1000)
        max_chunks: Max number of chunks. If set to 0 then splitting is disabled and one single
                    file will be created (Default: 20)
        est_compr_rate: Detecting compressed file size at write time is not possible by GzipFile.
                        The data hase to be written into the file first before the actual compression performed.
                        We need to use a good estimate for a text gzip file to split the file at write time.
                        (Default 0.12)

    Return:
        File like object
    """
    if mode not in ['wb', 'wt']:
        raise ValueError('Invalid mode: %r' % (mode,))
    if chunk_size_mb is not None and chunk_size_mb < 1:
        raise ValueError('Invalid chunk_size_mb: %d' % (chunk_size_mb,))
    if max_chunks is not None and max_chunks < 0:
        raise ValueError('Invalid max_chunks: %d' % (max_chunks,))
    return SplitGzipFile(base_filename, mode, chunk_size_mb, max_chunks, est_compr_rate)


# pylint: disable=R0902
class SplitGzipFile(io.BufferedIOBase):
    """The SplitGzipFile file like object class that implements only the write method.

    This class only supports writing files in binary mode.
    """
    def __init__(self,
                 base_filename,
                 mode: str = None,
                 chunk_size_mb: int = None,
                 max_chunks: int = None,
                 est_compr_rate: float = None):
        super().__init__()

        self.base_filename = base_filename
        self.mode = mode
        self.chunk_size_mb = chunk_size_mb or DEFAULT_CHUNK_SIZE_MB
        self.max_chunks = max_chunks if max_chunks is not None else DEFAULT_MAX_CHUNKS
        self.est_compr_rate = est_compr_rate if est_compr_rate is not None else EST_COMPR_RATE

        self.split_large_files = True
        self.chunk_seq = 1
        self.current_chunk_size_mb = 0
        self.chunk_filename = None
        self.chunk_file = None

    def _gen_chunk_filename(self) -> str:
        """
        Generates a chunk filename

        Pattern if max_chunks is zero: <base_filename>
        Pattern if max_chunks is greater than zero: <base_filename>.part<chunk-number-padded-five-digits>

        Returns:
            string
        """
        if self.max_chunks == 0:
            chunk_filename = self.base_filename
        else:
            if self.current_chunk_size_mb >= self.chunk_size_mb and self.chunk_seq < self.max_chunks:
                # Increase the chunk sequence and reset size to zero
                self.chunk_seq += 1
                self.current_chunk_size_mb = 0

            chunk_filename = f'{self.base_filename}.part{self.chunk_seq:05d}'

        return chunk_filename

    def _activate_chunk_file(self):
        """
        Activate a file like object to write data into the active chunk
        """
        chunk_filename = self._gen_chunk_filename()
        # Close the actual chunk file if exists and open a new one
        if self.chunk_filename != chunk_filename:
            if self.chunk_file:
                self.chunk_file.close()

            # Open the actual chunk file with gzip data writer
            self.chunk_filename = chunk_filename
            self.chunk_file = gzip.open(self.chunk_filename, self.mode)

    @staticmethod
    def _bytes_to_megabytes(size: int) -> float:
        """
        Transforms bytes ot megabytes

        Args:
            size: Number of bytes
        Returns:
            floating point number number
        """
        return size / float(1 << 20)

    def write(self, b):
        self._activate_chunk_file()

        self.chunk_file.write(b)
        self.current_chunk_size_mb = SplitGzipFile._bytes_to_megabytes(self.chunk_file.tell() * self.est_compr_rate)

    def close(self):
        if self.chunk_file is None:
            return
        self.chunk_file.close()

    def flush(self):
        self.chunk_file.flush()
