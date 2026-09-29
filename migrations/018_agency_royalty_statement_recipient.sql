ALTER TABLE public.agency_profiles
ADD COLUMN IF NOT EXISTS royalty_statement_recipient_name text,
ADD COLUMN IF NOT EXISTS royalty_statement_recipient_email text;
