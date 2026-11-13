import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import DEFAULT, Mock, patch

# Tests never use local credentials or connect to a database.
with patch.dict(os.environ, {
    "DB": "test_db",
    "SRC_HOST": "source.invalid",
    "SRC_USER": "reader",
    "SRC_PASSWORD": "test-only",
    "SRC_PORT": "3306",
    "DST_HOST": "target.invalid",
    "DST_USER": "writer",
    "DST_PASSWORD": "test-only",
    "DST_PORT": "3306",
}):
    import cdc_sync as cdc


class SaveStateTests(unittest.TestCase):
    def test_create_and_replace_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            with patch.object(cdc, "STATE_FILE", str(state)):
                for pos in (4, 100):
                    cdc.save_state("bin.000001", pos)
                    self.assertEqual(json.loads(state.read_text()),
                                     {"file": "bin.000001", "pos": pos})
                    self.assertFalse(Path(str(state) + ".tmp").exists())

    def test_transient_permission_error_retries_atomic_replace(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            state.write_text('{"file": "bin.000001", "pos": 4}')
            with patch.object(cdc, "STATE_FILE", str(state)), \
                    patch.object(cdc.os, "replace", wraps=os.replace,
                                 side_effect=[PermissionError("locked"), DEFAULT]) as replace, \
                    patch.object(cdc.time, "sleep") as sleep, \
                    patch("builtins.print") as warning:
                cdc.save_state("bin.000001", 100)
            self.assertEqual(replace.call_count, 2)
            sleep.assert_called_once_with(0.1)
            warning.assert_called_once()
            self.assertEqual(json.loads(state.read_text()),
                             {"file": "bin.000001", "pos": 100})
            self.assertFalse(Path(str(state) + ".tmp").exists())

    def test_persistent_permission_error_preserves_checkpoint_and_temp(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            original = '{"file": "bin.000001", "pos": 4}'
            state.write_text(original)
            error = PermissionError("locked")
            with patch.object(cdc, "STATE_FILE", str(state)), \
                    patch.object(cdc.os, "replace", side_effect=error) as replace, \
                    patch.object(cdc.time, "sleep") as sleep, \
                    patch("builtins.print"):
                with self.assertRaisesRegex(PermissionError, "checkpoint was not advanced") as raised:
                    cdc.save_state("bin.000001", 100)
            self.assertIs(raised.exception.__cause__, error)
            self.assertEqual(replace.call_count, 5)
            self.assertEqual([call.args[0] for call in sleep.call_args_list],
                             [0.1, 0.2, 0.4, 0.8])
            self.assertEqual(state.read_text(), original)
            self.assertEqual(json.loads(Path(str(state) + ".tmp").read_text()),
                             {"file": "bin.000001", "pos": 100})

    def test_other_os_errors_are_not_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            with patch.object(cdc, "STATE_FILE", str(state)), \
                    patch.object(cdc.os, "replace", side_effect=OSError("disk error")) as replace, \
                    patch.object(cdc.time, "sleep") as sleep:
                with self.assertRaisesRegex(OSError, "disk error"):
                    cdc.save_state("bin.000001", 100)
            replace.assert_called_once()
            sleep.assert_not_called()


class ApplyEventTests(unittest.TestCase):
    def test_insert_and_replay_for_all_key_types(self):
        examples = {
            "customer": {"customer_no": "C001", "name": "Alice"},
            "orders": {"order_no": "O1001", "amount": 10},
            "product": {"product_id": 101, "name": "Keyboard"},
            "order_item": {"order_id": 1001, "line_no": 1, "quantity": 2},
            "activity_log": {"my_row_id": 7, "event_type": "LOGIN", "details": "Signed in"},
        }
        for table, values in examples.items():
            with self.subTest(table=table):
                cur = Mock()
                event = Mock(spec=cdc.WriteRowsEvent, table=table, rows=[{"values": values}])
                cdc.apply_event(cur, event)
                cdc.apply_event(cur, event)
                columns = ", ".join(f"`{name}`" for name in values)
                placeholders = ", ".join(["%s"] * len(values))
                updates = ", ".join(f"`{name}`=VALUES(`{name}`)" for name in values)
                expected = (
                    f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders}) "
                    f"ON DUPLICATE KEY UPDATE {updates}"
                )
                self.assertEqual(cur.execute.call_count, 2)
                for call in cur.execute.call_args_list:
                    self.assertEqual(call.args, (expected, list(values.values())))

    def test_update_uses_old_single_or_composite_key(self):
        examples = [
            ("activity_log", {"my_row_id": 7, "details": "Signed in"},
             {"my_row_id": 7, "details": "Session started"}, "`my_row_id`=%s", [7]),
            ("customer", {"customer_no": "C001", "name": "Alice"},
             {"customer_no": "C001X", "name": "Alice"}, "`customer_no`=%s", ["C001"]),
            ("orders", {"order_no": "O1001", "amount": 10},
             {"order_no": "O1001X", "amount": 20}, "`order_no`=%s", ["O1001"]),
            ("product", {"product_id": 101, "name": "Keyboard"},
             {"product_id": 202, "name": "New keyboard"}, "`product_id`=%s", [101]),
            ("order_item", {"line_no": 1, "order_id": 1001, "quantity": 2},
             {"line_no": 3, "order_id": 2001, "quantity": 4},
             "`order_id`=%s AND `line_no`=%s", [1001, 1]),
        ]
        for table, before, after, where, key in examples:
            with self.subTest(table=table):
                cur = Mock()
                event = Mock(spec=cdc.UpdateRowsEvent, table=table,
                             rows=[{"before_values": before, "after_values": after}])
                cdc.apply_event(cur, event)
                sets = ", ".join(f"`{name}`=%s" for name in after)
                cur.execute.assert_called_once_with(
                    f"UPDATE `{table}` SET {sets} WHERE {where}", list(after.values()) + key)

    def test_delete_uses_only_configured_key(self):
        examples = [
            ("activity_log", {"my_row_id": 7, "event_type": "LOGIN"}, "`my_row_id`=%s", [7]),
            ("customer", {"customer_no": "C001", "name": "Alice"}, "`customer_no`=%s", ["C001"]),
            ("orders", {"order_no": "O1001", "amount": 10}, "`order_no`=%s", ["O1001"]),
            ("product", {"product_id": 101, "name": "Keyboard"}, "`product_id`=%s", [101]),
            ("order_item", {"line_no": 2, "order_id": 1001, "quantity": 4},
             "`order_id`=%s AND `line_no`=%s", [1001, 2]),
        ]
        for table, values, where, key in examples:
            with self.subTest(table=table):
                cur = Mock()
                event = Mock(spec=cdc.DeleteRowsEvent, table=table, rows=[{"values": values}])
                cdc.apply_event(cur, event)
                cur.execute.assert_called_once_with(f"DELETE FROM `{table}` WHERE {where}", key)

    def test_multi_row_event(self):
        cur = Mock()
        event = Mock(spec=cdc.DeleteRowsEvent, table="order_item",
                     rows=[{"values": {"order_id": 1001, "line_no": line}} for line in (1, 2)])
        cdc.apply_event(cur, event)
        self.assertEqual([call.args[1] for call in cur.execute.call_args_list],
                         [[1001, 1], [1001, 2]])

    def test_missing_or_null_key_fails_before_execution(self):
        for values in ({"order_id": 1001}, {"order_id": 1001, "line_no": None}):
            for event_type in (cdc.WriteRowsEvent, cdc.UpdateRowsEvent, cdc.DeleteRowsEvent):
                for bad_before in (True, False):
                    with self.subTest(values=values, event=event_type, bad_before=bad_before):
                        valid = {"order_id": 1001, "line_no": 1}
                        row = {"values": values,
                               "before_values": values if bad_before else valid,
                               "after_values": valid if bad_before else values}
                        event = Mock(spec=event_type, table="order_item", rows=[row])
                        cur = Mock()
                        with self.assertRaisesRegex(ValueError, "order_item: key column line_no"):
                            cdc.apply_event(cur, event)
                        cur.execute.assert_not_called()

    def test_unconfigured_table_is_ignored(self):
        cur = Mock()
        event = Mock(spec=cdc.DeleteRowsEvent, table="other", rows=[{"values": {}}])
        cdc.apply_event(cur, event)
        cur.execute.assert_not_called()

    def test_stream_includes_pk_and_unique_key_tables(self):
        stream = Mock()
        stream.__iter__ = Mock(return_value=iter([]))
        dst = Mock()
        with patch.object(cdc, "load_state", return_value={"file": "bin.000001", "pos": 4}), \
                patch.object(cdc, "BinLogStreamReader", return_value=stream) as reader, \
                patch.object(cdc.pymysql, "connect", return_value=dst):
            cdc.main()
        self.assertEqual(set(reader.call_args.kwargs["only_tables"]),
                         {"customer", "orders", "product", "order_item", "activity_log"})
        stream.close.assert_called_once()
        dst.close.assert_called_once()

    def test_identifier_escaping(self):
        self.assertEqual(cdc.quote_identifier("odd`column"), "`odd``column`")


if __name__ == "__main__":
    unittest.main()
