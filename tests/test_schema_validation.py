import sys
import os

from src.validation.schema_validation import MemberSchema, RawMemberSchema

print('Testing schema_validation...')

def test_phone_validator():
	print('Test phone_validator....')

	#input, output, success/failure
	test_phone_number = [
		("(123) 234-2345", "123-234-2345", True),
		("1234567890", "123-456-7890", True)	]

	result_pass = True
	for input, output, isSuccess in test_phone_number:

		#member_id, first_name, last_name, dob, gender, phone, email, zip5, plan_id
		member = MemberSchema(
			member_id="member123",
			first_name="first",
			last_name="last",
			dob="2000-01-01",
			gender="F",
			phone=input,
			zip5="94024",
			plan_id="PLAN_A"
			)

		if isSuccess:
			if member.phone == output:
				result_pass = True
				print('PHONE ---- TEST CASE PASS')
			else:
				result_pass = False
				print('PHONE ---- TEST CASE FAIL')

	return result_pass

def test_zip5_validator():
	print('Test zip5 validator')

	#input, output, success/failure
	test_zip5 = [
		("94024", "94024", True)
	]

	result_pass = True
	for input, output, isSuccess in test_zip5:
		member = MemberSchema(
			member_id="member123",
			first_name="first",
			last_name="last",
			dob="2000-01-01",
			gender="F",
			phone="123-123-1234",
			zip5=input,
			plan_id="PLAN_A"

			)
		if isSuccess:
			if member.zip5 == output:
				result_pass = True
				print('ZIP5 ---- TEST CASE PASS')
			else:
				result_pass = False
				print('ZIP5 ---- TEST CASE FAIL')


result_phone_validator = test_phone_validator()
result_zip5_validator = test_zip5_validator()
print(result_phone_validator)
print(result_zip5_validator)

