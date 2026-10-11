from decimal import Decimal, ROUND_HALF_UP
from fastapi import HTTPException

ZERO = Decimal('0.00')


def money(value):
    result = Decimal(str(value)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
    if not result.is_finite():
        raise ValueError('Money must be finite')
    return result


def split_amount(amount, instruction):
    amount = money(amount)
    cp, ap = [Decimal(str(instruction[k])) for k in ('contributor_percent', 'agency_percent')]
    mode = instruction['payee_mode']
    if (amount <= ZERO or not cp.is_finite() or not ap.is_finite() or
            cp < 0 or ap < 0 or cp+ap != 100 or
            mode not in ('contributor_only','agency_only','split') or
            (mode == 'contributor_only' and (cp,ap) != (100,0)) or
            (mode == 'agency_only' and (cp,ap) != (0,100)) or
            not instruction.get('contributor_party_id') or
            (ap > 0 and not instruction.get('agency_party_id')) or
            instruction.get('contributor_party_id') == instruction.get('agency_party_id')):
        raise HTTPException(409, 'Missing or contradictory recipient instructions.')
    contributor = money(amount*cp/100)
    # Assign the deterministic rounding remainder to the agency.
    return [(role, instruction[key], percent, value)
            for role,key,percent,value in (
                ('contributor','contributor_party_id',cp,contributor),
                ('agency','agency_party_id',ap,amount-contributor)) if value > ZERO]


def next_status(current, incoming):
    allowed = {
        'created': {'submitted','failed','canceled'},
        'submitted': {'processing','settled','failed','returned'},
        'processing': {'settled','failed','returned'},
        'settled': {'returned'},
        'returned': set(), 'failed': set(), 'canceled': set(),
    }
    return incoming if incoming in allowed.get(current,set()) else current
