-- Incremental messaging extension. Apply explicitly after 007; never on startup.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
ALTER TABLE public.marketplace_messages ADD COLUMN client_message_id uuid;
CREATE UNIQUE INDEX marketplace_messages_idempotency ON public.marketplace_messages(sender_actor_id,client_message_id) WHERE client_message_id IS NOT NULL;
ALTER TABLE public.marketplace_conversation_participants ADD COLUMN last_read_message_id uuid;
CREATE INDEX marketplace_conversations_activity ON public.marketplace_conversations(last_message_at DESC NULLS LAST,created_at DESC,id);
CREATE TABLE public.marketplace_messaging_sessions (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 actor_id uuid NOT NULL REFERENCES public.marketplace_actors(id) ON DELETE CASCADE,
 user_id uuid NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
 ticket_hash text UNIQUE,
 ticket_expires_at timestamptz NOT NULL DEFAULT now()+interval '60 seconds',
 connected boolean NOT NULL DEFAULT false,
 heartbeat_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX marketplace_messaging_presence ON public.marketplace_messaging_sessions(actor_id,heartbeat_at) WHERE connected;
CREATE FUNCTION public.marketplace_messaging_notify() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE c public.marketplace_conversations;
BEGIN
 SELECT * INTO c FROM public.marketplace_conversations WHERE id=NEW.conversation_id;
 PERFORM pg_notify('marketplace_messaging',json_build_object('type','refresh','conversation_id',c.id,'actors',ARRAY[c.actor_low_id,c.actor_high_id],
   'incoming',TG_TABLE_NAME='marketplace_messages' AND TG_OP='INSERT',
   'sender_actor_id',CASE WHEN TG_TABLE_NAME='marketplace_messages' THEN to_jsonb(NEW)->>'sender_actor_id' ELSE NULL END)::text);
 RETURN NEW;
END $$;
CREATE TRIGGER marketplace_message_notify AFTER INSERT OR UPDATE OF status ON public.marketplace_messages FOR EACH ROW EXECUTE FUNCTION public.marketplace_messaging_notify();
CREATE TRIGGER marketplace_read_notify AFTER UPDATE OF last_read_at,last_read_message_id ON public.marketplace_conversation_participants FOR EACH ROW WHEN (OLD.last_read_at IS DISTINCT FROM NEW.last_read_at OR OLD.last_read_message_id IS DISTINCT FROM NEW.last_read_message_id) EXECUTE FUNCTION public.marketplace_messaging_notify();
COMMIT;
