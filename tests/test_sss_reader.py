import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from app.sss_reader import FilePolicy, SecretReadError, read_secret_bytes, secret_text


class ReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.policy = FilePolicy(root=self.root, owner_uids=(os.getuid(),))
        self.file = self.root/'PASSWORD'
        self.file.write_bytes(b'  synthetic-value\n')
        self.file.chmod(0o600)

    def tearDown(self):
        self.temp.cleanup()

    def test_preserve_bytes(self):
        self.assertEqual(read_secret_bytes(self.file, self.policy), b'  synthetic-value\n')

    def test_file_text(self):
        self.assertEqual(secret_text('PASSWORD', environ={'PASSWORD_FILE':str(self.file)}, policy=self.policy), '  synthetic-value\n')

    def test_missing_fails(self):
        with self.assertRaisesRegex(SecretReadError, 'secret_reference_required:PASSWORD'):
            secret_text('PASSWORD', environ={}, policy=self.policy)

    def test_conflicting_sources_even_empty(self):
        with self.assertRaisesRegex(SecretReadError, 'secret_sources_conflict'):
            secret_text('PASSWORD', environ={'PASSWORD':'','PASSWORD_FILE':str(self.file)}, policy=self.policy)

    def test_literal_rejected_by_default(self):
        with self.assertRaises(SecretReadError):
            secret_text('PASSWORD', environ={'PASSWORD':'synthetic'})

    def test_explicit_legacy_transition(self):
        self.assertEqual(secret_text('PASSWORD', environ={'PASSWORD':'synthetic'}, allow_legacy_environment=True), 'synthetic')

    def test_enforcement_rejects_legacy(self):
        with self.assertRaises(SecretReadError):
            secret_text('PASSWORD', environ={'PASSWORD':'synthetic','SSS_ENFORCED':'1'}, allow_legacy_environment=True)

    def test_invalid_enforcement_fails_closed(self):
        with self.assertRaises(SecretReadError):
            secret_text('PASSWORD', environ={'PASSWORD':'synthetic','SSS_ENFORCED':'typo'}, allow_legacy_environment=True)

    def test_file_symlink_rejected(self):
        link=self.root/'link'
        link.symlink_to(self.file)
        with self.assertRaises(SecretReadError):
            read_secret_bytes(link,self.policy)

    def test_directory_symlink_rejected(self):
        child=self.root/'real'
        child.mkdir()
        (child/'value').write_bytes(b'fake')
        (child/'value').chmod(0o600)
        (self.root/'link').symlink_to(child)
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.root/'link/value',self.policy)

    def test_root_symlink_rejected(self):
        (self.root/'alias').symlink_to(self.root)
        policy=FilePolicy(root=self.root/'alias',owner_uids=(os.getuid(),))
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.root/'alias/PASSWORD',policy)

    def test_world_readable_rejected(self):
        self.file.chmod(0o644)
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.file,self.policy)

    def test_group_read_rejected_without_explicit_policy(self):
        self.file.chmod(0o640)
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.file,self.policy)

    def test_group_read_requires_matching_gid(self):
        self.file.chmod(0o640)
        permitted=FilePolicy(root=self.root,owner_uids=(os.getuid(),),readable_gid=os.getgid())
        self.assertTrue(read_secret_bytes(self.file,permitted))
        wrong=FilePolicy(root=self.root,owner_uids=(os.getuid(),),readable_gid=os.getgid()+1)
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.file,wrong)

    def test_wrong_owner(self):
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.file,FilePolicy(root=self.root,owner_uids=(os.getuid()+1,)))

    def test_writable_directory_rejected(self):
        self.root.chmod(0o777)
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.file,self.policy)

    def test_hardlink_rejected(self):
        os.link(self.file,self.root/'hardlink')
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.file,self.policy)

    def test_fifo_does_not_block(self):
        fifo=self.root/'fifo'
        os.mkfifo(fifo,0o600)
        with self.assertRaises(SecretReadError):
            read_secret_bytes(fifo,self.policy)

    def test_empty_rejected(self):
        self.file.write_bytes(b'')
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.file,self.policy)

    def test_size_limit(self):
        with self.assertRaises(SecretReadError):
            read_secret_bytes(self.file,FilePolicy(root=self.root,owner_uids=(os.getuid(),),maximum_bytes=2))

    def test_non_utf8_available_as_bytes_not_text(self):
        self.file.write_bytes(b'\xff\x00')
        self.assertEqual(read_secret_bytes(self.file,self.policy),b'\xff\x00')
        with self.assertRaisesRegex(SecretReadError,'secret_encoding_rejected'):
            secret_text('PASSWORD',environ={'PASSWORD_FILE':str(self.file)},policy=self.policy)

    def test_outside_root_and_traversal(self):
        for path in (self.root.parent/'other', self.root/'../other'):
            with self.assertRaises(SecretReadError):
                read_secret_bytes(path,self.policy)

    def test_fixed_errors_do_not_echo_values_or_path(self):
        with self.assertRaises(SecretReadError) as caught:
            secret_text('PASSWORD',environ={'PASSWORD_FILE':'/sensitive/path'},policy=self.policy)
        self.assertNotIn('/sensitive/path',str(caught.exception))
        self.assertNotIn('synthetic-value',str(caught.exception))

    def test_invalid_name_not_echoed(self):
        with self.assertRaises(SecretReadError) as caught:
            secret_text('bad=sensitive')
        self.assertEqual(str(caught.exception),'invalid_secret_name')

    def test_nested_writable_directory_rejected(self):
        child = self.root/'nested'
        child.mkdir(mode=0o700)
        nested = child/'PASSWORD'
        nested.write_bytes(b'synthetic')
        nested.chmod(0o600)
        child.chmod(0o770)
        with self.assertRaises(SecretReadError):
            read_secret_bytes(nested, self.policy)

    def test_directory_not_accepted_as_material(self):
        child = self.root/'directory'
        child.mkdir(mode=0o700)
        with self.assertRaises(SecretReadError):
            read_secret_bytes(child, self.policy)

    def test_executable_and_group_writable_material_rejected(self):
        for mode in (0o700, 0o660, 0o602):
            with self.subTest(mode=mode):
                self.file.chmod(mode)
                with self.assertRaises(SecretReadError):
                    read_secret_bytes(self.file, self.policy)

    def test_invalid_policy_roots(self):
        for root in (Path('/'), Path('relative'), self.root/'..'):
            with self.subTest(root=root):
                with self.assertRaises(SecretReadError):
                    read_secret_bytes(self.file, FilePolicy(root=root, owner_uids=(os.getuid(),)))

    def test_zero_limit_and_empty_owners_rejected(self):
        for policy in (FilePolicy(root=self.root, maximum_bytes=0),
                       FilePolicy(root=self.root, owner_uids=())):
            with self.assertRaises(SecretReadError):
                read_secret_bytes(self.file, policy)

    def test_empty_file_reference_rejected(self):
        with self.assertRaisesRegex(SecretReadError, 'secret_file_unavailable'):
            secret_text('PASSWORD', environ={'PASSWORD_FILE':''}, policy=self.policy)

    def test_legacy_size_limit(self):
        with self.assertRaisesRegex(SecretReadError, 'secret_file_size_rejected'):
            secret_text('PASSWORD', environ={'PASSWORD':'synthetic'},
                        policy=FilePolicy(maximum_bytes=2), allow_legacy_environment=True)

    def test_growth_during_read_rejected(self):
        original_read = os.read
        changed = False
        def changing_read(fd, size):
            nonlocal changed
            if not changed:
                changed = True
                with self.file.open('ab') as stream:
                    stream.write(b' changed')
            return original_read(fd, size)
        with patch('app.sss_reader.os.read', side_effect=changing_read):
            with self.assertRaisesRegex(SecretReadError, 'secret_file_changed_during_read'):
                read_secret_bytes(self.file, self.policy)

    def test_shrink_during_read_rejected(self):
        original_read = os.read
        changed = False
        def changing_read(fd, size):
            nonlocal changed
            if not changed:
                changed = True
                self.file.write_bytes(b'x')
            return original_read(fd, size)
        with patch('app.sss_reader.os.read', side_effect=changing_read):
            with self.assertRaisesRegex(SecretReadError, 'secret_file_changed_during_read'):
                read_secret_bytes(self.file, self.policy)


if __name__=='__main__':
    unittest.main()
