from pydantic import BaseModel, EmailStr, Field, field_validator, ValidationError
from typing import Optional, List
from datetime import date, datetime
import re


class MemberSchema(BaseModel):
    """
    Normalized member data schema
    """

    # REQUIRED FIELDS
    member_id: str = Field(..., description="Unique member ID")
    first_name: str = Field(..., description="First name")
    last_name: str = Field(..., description="Last name")
    dob: date = Field(..., description="Date of birth in YYYY-MM-DD format")
    gender: str = Field(..., description="Gender M/F/O")
    phone: str = Field(..., description="Phone number in XXX-XXX-XXXX format")
    zip5: str = Field(..., description="5-digit zip code")
    plan_id: str = Field(..., description="Plan ID")

    # OPTIONAL
    email: Optional[EmailStr] = None

    # -------------------
    # Validators
    # -------------------

    @field_validator('dob', mode='before')
    @classmethod
    def parse_dob(cls, v):
        if isinstance(v, str):
            try:
                return datetime.strptime(v, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError("Invalid DOB format, expected YYYY-MM-DD")
        elif isinstance(v, date):
            return v
        else:
            raise ValueError("Invalid DOB type")

    @field_validator('gender')
    @classmethod
    def gender_validator(cls, v: str):
        valid_genders = ['M', 'F', 'O']
        if v.upper() not in valid_genders:
            raise ValueError("Invalid gender, must be M/F/O")
        return v.upper()

    @field_validator('phone')
    @classmethod
    def phone_validator(cls, v: str):
        digits = re.sub(r'\D', '', v)
        if len(digits) != 10:
            raise ValueError("Invalid phone number: must have 10 digits")
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"

    @field_validator('zip5')
    @classmethod
    def zip5_validator(cls, v: str):
        digits = re.sub(r'\D', '', v)
        if '-' in v:
        	v = v.strip('-')[0]

        if len(digits) != 5 or not v.isdigit():
            raise ValueError("Invalid zip code: must have 5 digits")
        return digits

    @field_validator('email')
    @classmethod
    def email_validator(cls, v):
        if v and '@' not in v:
            raise ValueError("Invalid email format")
        return v


class RawMemberSchema:
    """
    Process raw CSV/dict data and normalize into MemberSchema.
    Keeps track of validation errors without raising exceptions immediately.
    """

    def __init__(self, raw_data: dict):
        self.raw_data = raw_data
        self.errors: List[str] = []

    def normalize(self) -> Optional[MemberSchema]:
        # Strip and prepare raw data
        normalized_data = {
            'member_id': str(self.raw_data.get('member_id', '')).strip(),
            'first_name': str(self.raw_data.get('first_name', '')).strip(),
            'last_name': str(self.raw_data.get('last_name', '')).strip(),
            'dob': str(self.raw_data.get('dob', '')).strip(),
            'gender': str(self.raw_data.get('gender', '')).strip(),
            'phone': str(self.raw_data.get('phone', '')).strip(),
            'email': str(self.raw_data.get('email', '')).strip(),
            'zip5': str(self.raw_data.get('zip5', self.raw_data.get('zip_code', ''))).strip(),
            'plan_id': str(self.raw_data.get('plan_id', '')).strip()
        }

        # Check required fields first
        for field in ['member_id', 'first_name', 'last_name']:
            if not normalized_data[field]:
                self.errors.append(f"{field} is required")

        if self.errors:
            return None

        # Validate using MemberSchema
        try:
            member = MemberSchema(**normalized_data)
            return member
        except ValidationError as e:
            # Convert Pydantic errors to strings
            for err in e.errors():
                loc = ".".join(str(l) for l in err.get("loc", []))
                msg = err.get("msg", "")
                self.errors.append(f"{loc}: {msg}")
            return None
