-- Deal memos table (replaces TempDealMemo.json / S3).
-- Run after tenants, works, parties exist.
BEGIN;

CREATE TABLE IF NOT EXISTS deal_memos (
  id                           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id                    uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,

  work_id                      uuid NULL REFERENCES works(id) ON DELETE SET NULL,
  contributor_party_id         uuid NULL REFERENCES parties(id) ON DELETE SET NULL,
  agent_party_id               uuid NULL REFERENCES parties(id) ON DELETE SET NULL,

  uid                          text NOT NULL,
  name                         text NOT NULL DEFAULT '',
  title                        text NOT NULL DEFAULT '',
  contributor_role             text NOT NULL DEFAULT 'author',

  author                       text NOT NULL DEFAULT '',
  author_email                 text NOT NULL DEFAULT '',
  author_website               text NOT NULL DEFAULT '',
  author_phone_country_code    text NOT NULL DEFAULT '',
  author_phone_number          text NOT NULL DEFAULT '',
  author_address               jsonb NOT NULL DEFAULT '{}'::jsonb,
  author_birth_date            date,
  author_birth_city            text NOT NULL DEFAULT '',
  author_birth_country         text NOT NULL DEFAULT '',
  author_citizenship           text NOT NULL DEFAULT '',

  illustrator_name             text NOT NULL DEFAULT '',
  illustrator_email            text NOT NULL DEFAULT '',
  illustrator_website          text NOT NULL DEFAULT '',
  illustrator_phone_country_code text NOT NULL DEFAULT '',
  illustrator_phone_number     text NOT NULL DEFAULT '',
  illustrator_address          jsonb NOT NULL DEFAULT '{}'::jsonb,
  illustrator_birth_date        date,
  illustrator_birth_city        text NOT NULL DEFAULT '',
  illustrator_birth_country    text NOT NULL DEFAULT '',
  illustrator_citizenship      text NOT NULL DEFAULT '',

  agent_name                   text NOT NULL DEFAULT '',
  agency_name                  text NOT NULL DEFAULT '',
  agent_email                  text NOT NULL DEFAULT '',
  agency_website               text NOT NULL DEFAULT '',
  agency_street                text NOT NULL DEFAULT '',
  agency_city                  text NOT NULL DEFAULT '',
  agency_state                 text NOT NULL DEFAULT '',
  agency_zip                   text NOT NULL DEFAULT '',
  agency_country               text NOT NULL DEFAULT '',

  effective_date               date,
  projected_publication_date   text NOT NULL DEFAULT '',
  projected_retail_price       text NOT NULL DEFAULT '',
  territories_rights           text NOT NULL DEFAULT '',
  short_description            text NOT NULL DEFAULT '',

  option_deleted               boolean NOT NULL DEFAULT true,
  option_clause                text NOT NULL DEFAULT '',

  comp_copies_contributor      integer,
  comp_copies_agent            integer,

  author_advance               numeric(12,2),
  illustrator_advance          numeric(12,2),

  delivery_mode                text NOT NULL DEFAULT '',
  delivery_clause              text NOT NULL DEFAULT '',
  delivery_date                date,

  selected_template_id         text NOT NULL DEFAULT '',

  status                       text NOT NULL DEFAULT 'draft',
  generated_contract_s3_key    text NOT NULL DEFAULT '',
  generated_contract_filename  text NOT NULL DEFAULT '',
  generated_at                 timestamptz,

  advance_schedule             jsonb NOT NULL DEFAULT '[]'::jsonb,
  royalties                    jsonb NOT NULL DEFAULT '{}'::jsonb,

  payload_json                 jsonb NOT NULL DEFAULT '{}'::jsonb,

  source_created_at            timestamptz,
  source_updated_at            timestamptz,

  created_at                   timestamptz NOT NULL DEFAULT now(),
  updated_at                   timestamptz NOT NULL DEFAULT now(),

  UNIQUE (tenant_id, uid),

  CONSTRAINT chk_deal_memos_contributor_role
    CHECK (contributor_role IN ('author','illustrator','other')),

  CONSTRAINT chk_deal_memos_status
    CHECK (status IN ('draft','generated','archived'))
);

CREATE INDEX IF NOT EXISTS idx_deal_memos_tenant ON deal_memos(tenant_id);
CREATE INDEX IF NOT EXISTS idx_deal_memos_tenant_updated ON deal_memos(tenant_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_deal_memos_work ON deal_memos(work_id);
CREATE INDEX IF NOT EXISTS idx_deal_memos_royalties ON deal_memos USING GIN (royalties);
CREATE INDEX IF NOT EXISTS idx_deal_memos_advance_schedule ON deal_memos USING GIN (advance_schedule);

DROP TRIGGER IF EXISTS trg_touch_deal_memos ON deal_memos;
CREATE TRIGGER trg_touch_deal_memos
BEFORE UPDATE ON deal_memos
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

COMMIT;
