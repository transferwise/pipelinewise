import gzip
import itertools
import glob
import os
from unittest import TestCase

from pipelinewise.fastsync.commons import split_gzip


DATA_WITH_100_BYTES = b"""0,12345678
1,12345678
2,12345678
3,12345678
4,12345678
5,12345678
6,12345678
7,12345678
8,12345678
9,12345678
"""


def unlink(filename):
    """Helper function to delete file silently"""
    try:
        os.unlink(filename)
    except FileNotFoundError:
        pass


class SplitGzipFile(TestCase):
    """
    Unit tests for SplitGzipFileWriter
    """
    filename = '{}_{}_tmp'.format('@test', os.getpid())

    def setUp(self):
        unlink(self.filename)

    def tearDown(self):
        for temp_file in glob.glob('@test_*_tmp*'):
            unlink(temp_file)

    # pylint: disable=W0212
    def test_bytes_to_megabytes(self):
        """
        Test bytes to megabytes transformer
        """
        gzip_splitter = split_gzip.SplitGzipFile('foo')

        # Using binary unit
        self.assertEqual(gzip_splitter._bytes_to_megabytes(1024 ** 2), 1)
        self.assertEqual(gzip_splitter._bytes_to_megabytes(1024 ** 2 * 10), 10)
        self.assertEqual(gzip_splitter._bytes_to_megabytes(1024 ** 3), 1024)

        # Using SI kilo unit
        self.assertEqual(round(gzip_splitter._bytes_to_megabytes(1000), 5), 0.00095)
        self.assertEqual(round(gzip_splitter._bytes_to_megabytes(1000 ** 2 * 10), 5), 9.53674)
        self.assertEqual(round(gzip_splitter._bytes_to_megabytes(1000 ** 3), 5), 953.67432)

    def test_parameter_validation(self):
        """
        Test if passing invalid parameters raising exceptions
        """
        with self.assertRaises(ValueError):
            split_gzip.open('basefile', mode="invalidmode")
        with self.assertRaises(ValueError):
            split_gzip.open('basefile', mode='wt', chunk_size_mb=0)
        with self.assertRaises(ValueError):
            split_gzip.open('basefile', max_chunks=-1)

    # pylint: disable=W0212
    def test_gen_export_chunk_filename(self):
        """
        Test generating chunked filenames
        """
        # split_large_files should be disabled when max_chunks is zero
        gzip_splitter = split_gzip.SplitGzipFile('basefile', chunk_size_mb=1000, max_chunks=0)
        self.assertEqual(gzip_splitter._gen_chunk_filename(), 'basefile')
        # first chunk should be part nr 1
        gzip_splitter = split_gzip.SplitGzipFile('basefile', chunk_size_mb=1000, max_chunks=20)
        self.assertEqual(gzip_splitter._gen_chunk_filename(), 'basefile.part00001')
        # generated file part should be in sync with chunk_seq
        gzip_splitter.chunk_seq = 5
        self.assertEqual(gzip_splitter._gen_chunk_filename(), 'basefile.part00005')
        # chunk seq should not increase if chunk_size is lower than split_file_chunk_size_mb
        gzip_splitter.current_chunk_size_mb = 500
        self.assertEqual(gzip_splitter._gen_chunk_filename(), 'basefile.part00005')
        # chunk seq should increase and size should reset if current_chunk_size_mb greater than chunk_size_mb
        gzip_splitter.current_chunk_size_mb = 1050
        self.assertEqual(gzip_splitter._gen_chunk_filename(), 'basefile.part00006')
        self.assertEqual(gzip_splitter.current_chunk_size_mb, 0)
        # chunk seq should not increase further if chunk_seq equals to split_file_max_chunks
        gzip_splitter.chunk_seq = 20
        self.assertEqual(gzip_splitter._gen_chunk_filename(), 'basefile.part00020')

    def test_write_with_no_split(self):
        """
        Write gzip without splitting it and reading it
        """
        # max_chunk = 0 should create a file with no splitting
        with split_gzip.SplitGzipFile(self.filename, 'wb', max_chunks=0) as f_write:
            f_write.write(DATA_WITH_100_BYTES * 50)

        with gzip.open(self.filename, 'rb') as f_read:
            file_content = f_read.read()

        self.assertEqual(file_content, DATA_WITH_100_BYTES * 50)

    def test_write_with_single_chunk(self):
        """
        Write all data into one chunk
        """
        # test data fits into one chunk
        with split_gzip.SplitGzipFile(self.filename, 'wb', chunk_size_mb=1000, max_chunks=20) as f_write:
            f_write.write(DATA_WITH_100_BYTES * 50)

        with gzip.open(f'{self.filename}.part00001', 'rb') as f_read:
            file_content = f_read.read()

        self.assertEqual(file_content, DATA_WITH_100_BYTES * 50)

    def test_write_with_multiple_chunks(self):
        """
        Write data into multiple gzip files
        """
        # test data fits into one chunk
        with split_gzip.SplitGzipFile(self.filename, 'wb',
                                      chunk_size_mb=split_gzip.SplitGzipFile._bytes_to_megabytes(200),
                                      max_chunks=20,
                                      est_compr_rate=1) as f_write:
            # Write 1100 bytes of test data
            for _ in itertools.repeat(None, 11):
                f_write.write(DATA_WITH_100_BYTES)

        # Result should be in 6 gzip files
        with gzip.open(f'{self.filename}.part00001', 'rb') as f_read:
            self.assertEqual(f_read.read(), DATA_WITH_100_BYTES * 2)
        with gzip.open(f'{self.filename}.part00002', 'rb') as f_read:
            self.assertEqual(f_read.read(), DATA_WITH_100_BYTES * 2)
        with gzip.open(f'{self.filename}.part00003', 'rb') as f_read:
            self.assertEqual(f_read.read(), DATA_WITH_100_BYTES * 2)
        with gzip.open(f'{self.filename}.part00004', 'rb') as f_read:
            self.assertEqual(f_read.read(), DATA_WITH_100_BYTES * 2)
        with gzip.open(f'{self.filename}.part00005', 'rb') as f_read:
            self.assertEqual(f_read.read(), DATA_WITH_100_BYTES * 2)
        # Last chunk should be smaller
        with gzip.open(f'{self.filename}.part00006', 'rb') as f_read:
            self.assertEqual(f_read.read(), DATA_WITH_100_BYTES)
