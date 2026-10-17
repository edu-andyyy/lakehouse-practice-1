-- Snapshot from this run, read without changing the table
SELECT count(*) FROM lakehouse.sandbox.shipping_lines_demo FOR VERSION AS OF 384267507252458803;
-- Restore the existing demo table to that snapshot
ALTER TABLE lakehouse.sandbox.shipping_lines_demo EXECUTE rollback_to_snapshot(384267507252458803);
