import pytest
from validation.schema_validation import RawMemberSchema, MemberSchema

class TestValidation:

    def test_valid_member_record(self):
        """Test valid member record normalization"""
        raw_data = {
            'member_id': '12345',
            'first_name': 'John',
            'last_name': 'Doe',
            'dob': '1980-01-15',
            'gender': 'M',
            'phone': '555-123-4567',
            'email': 'john.doe@example.com',
            'zip5': '94105',
            'plan_id': 'PLAN_A'
        }

        record = RawMemberSchema(raw_data)
        normalized = record.normalize()

        # Ensure no errors
        assert normalized is not None
        assert isinstance(normalized, MemberSchema)
        assert not record.errors

    def test_invalid_phone_number(self):
        """Test record with invalid phone number"""
        raw_data = {
            'member_id': '12345',
            'first_name': 'John',
            'last_name': 'Doe',
            'dob': '1980-01-15',
            'gender': 'M',
            'phone': '123',  # Invalid
            'email': 'john.doe@example.com',
            'zip5': '94105',
            'plan_id': 'PLAN_A'
        }

        record = RawMemberSchema(raw_data)
        normalized = record.normalize()

        assert normalized is None
        assert len(record.errors) > 0
        # Should contain a phone error
        assert any("phone" in e.lower() and "invalid phone" in e.lower() for e in record.errors)

    def test_missing_required_field(self):
        """Test record with missing required field"""
        raw_data = {
            'member_id': '12345',
            'first_name': 'John',
            # last_name missing
            'dob': '1980-01-15',
            'gender': 'M',
            'phone': '555-123-4567',
            'zip5': '94105',
            'plan_id': 'PLAN_A'
        }

        record = RawMemberSchema(raw_data)
        normalized = record.normalize()

        assert normalized is None
        assert len(record.errors) > 0
        # Should contain a last_name required error
        assert any("last_name" in e.lower() and "required" in e.lower() for e in record.errors)
