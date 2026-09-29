CREATE TABLE IF NOT EXISTS secure_payments.agency_information_requests (
 request_id uuid PRIMARY KEY REFERENCES secure_payments.payment_requests(id) ON DELETE CASCADE,
 tenant_id uuid NOT NULL REFERENCES public.tenants(id),
 work_id uuid NOT NULL REFERENCES public.works(id),
 contributor_party_id uuid NOT NULL REFERENCES public.parties(id),
 agency_party_id uuid REFERENCES public.parties(id),
 agent_party_id uuid REFERENCES public.parties(id)
);
CREATE INDEX IF NOT EXISTS agency_information_requests_work_idx ON secure_payments.agency_information_requests(tenant_id,work_id);
