import os
import pandas as pd

# Load all CSVs
main_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
users_df = pd.read_csv(os.path.join(main_dir, 'RAW', 'data', 'raw_User.csv'))
departments_df = pd.read_csv(os.path.join(main_dir, 'RAW', 'data', 'raw_Department.csv'))
skills_df = pd.read_csv(os.path.join(main_dir, 'RAW', 'data', 'raw_Skill.csv'))
skills_department_df = pd.read_csv(os.path.join(main_dir, 'RAW', 'data', 'raw_SkillDepartment.csv'))
skill_users_df = pd.read_csv(os.path.join(main_dir, 'RAW', 'data', 'raw_SkillUsers.csv'))
courses_df = pd.read_csv(os.path.join(main_dir, 'RAW', 'data', 'raw_Course.csv'))
course_department_df = pd.read_csv(os.path.join(main_dir, 'RAW', 'data', 'raw_CourseDepartment.csv'))
course_users_df = pd.read_csv(os.path.join(main_dir, 'RAW', 'data', 'raw_CourseUser.csv'))

# Prepping User data

# Dropping PPI
users_df = users_df.drop(['email', 'password'], axis=1)

# Merging department name and id
users_df = pd.merge(users_df, departments_df, on='dept_id', how='left')

# Removing unnecssary columns
users_df = users_df[['user_id', 'first_name', 'last_name', 'account_type', 'dept_name', 'createdAt', 'updatedAt']]

# Cleaning and filling null values
users_df['dept_name'] = users_df['dept_name'].fillna('Unknown')
users_df['first_name'] = users_df['first_name'].str.strip()
users_df['last_name'] = users_df['last_name'].str.strip()

# Making column names consistent
users_df = users_df.rename(columns={
    'dept_name': 'department_name'  
})

departments_df = departments_df.rename(columns={
    'dept_id': 'department_id',
    'dept_name': 'department_name'
})

skills_department_df = skills_department_df.rename(columns= {
    'dept_id': 'department_id'
})

courses_df = courses_df.rename(columns={
    'course_desc':'course_descripition',
    'course_creator': 'user_id',
    'course_img': 'course_image'
})

course_department_df = course_department_df.rename(columns={
    'dept_id': 'department_id'
})

# Selecting only needed columns
departments_df = departments_df[['department_id', 'department_name']]
skills_df = skills_df[['skill_id', 'skill_name']]
skills_department_df = skills_department_df[['skill_id', 'department_id']]
skill_users_df = skill_users_df[['skill_id', 'user_id', 'competency']]
courses_df = courses_df[['course_id','course_name', 'course_descripition', 'user_id', 'createdAt', 'updatedAt']]
course_department_df = course_department_df[['course_id', 'department_id']]
course_users_df = course_users_df[['course_id', 'user_id', 'user_score']]

users_df.to_csv('prep_Users.csv', index=False)
departments_df.to_csv('prep_Departments.csv', index=False)
skills_df.to_csv('prep_Skills.csv', index=False)
skills_department_df.to_csv('prep_SkillDepartment.csv', index=False)
skill_users_df.to_csv('prep_SkillUsers.csv', index=False)
courses_df.to_csv('prep_Courses.csv', index=False)
course_department_df.to_csv('prep_CourseDepartment.csv', index=False)
course_users_df.to_csv('prep_CourseUser.csv', index=False)