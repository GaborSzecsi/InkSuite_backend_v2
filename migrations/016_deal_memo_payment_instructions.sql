ALTER TABLE public.deal_memo_drafts ADD COLUMN IF NOT EXISTS payment_instructions jsonb;
