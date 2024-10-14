import pandas as pd
import os 


# Load DataFrames (adjust paths as needed)
main_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
users_df = pd.read_csv(os.path.join(main_dir, 'PREP', 'data', 'prep_Users.csv'))
departments_df = pd.read_csv(os.path.join(main_dir, 'PREP', 'data', 'prep_Departments.csv'))
skills_df = pd.read_csv(os.path.join(main_dir, 'PREP', 'data', 'prep_Skills.csv'))
skill_departments_df = pd.read_csv(os.path.join(main_dir, 'PREP', 'data', 'prep_SkillDepartment.csv'))
skill_users_df = pd.read_csv(os.path.join(main_dir, 'PREP', 'data', 'prep_SkillUsers.csv'))
courses_df = pd.read_csv(os.path.join(main_dir, 'PREP', 'data', 'prep_Courses.csv'))
course_departments_df = pd.read_csv(os.path.join(main_dir, 'PREP', 'data', 'prep_CourseDepartment.csv'))
course_users_df = pd.read_csv(os.path.join(main_dir, 'PREP', 'data', 'prep_CourseUser.csv'))


# Step 1: Create Dimension Tables

# DimUser
dim_user = users_df[['user_id', 'first_name', 'last_name', 'account_type', 'department_name', 'createdAt', 'updatedAt']]

# DimDepartment
dim_department = departments_df[['department_id', 'department_name']]

# DimSkill
dim_skill = skills_df[['skill_id', 'skill_name']]

# DimCourse
dim_course = courses_df[['course_id', 'course_name', 'course_descripition', 'user_id', 'createdAt', 'updatedAt']]


# FactSkillUser: Merge skill_users with dim_user, and dim_skill to get full information
fact_skill_user = skill_users_df.merge(dim_user, on='user_id', how='inner')\
                                .merge(dim_skill, on='skill_id', how='inner')\
                                .merge(skill_departments_df, on='skill_id', how='inner')\
                                .merge(dim_department, on='department_id', how='inner')

fact_skill_user = fact_skill_user[['skill_id', 'skill_name', 'user_id', 'first_name', 'last_name', 'account_type', 'department_id', 'department_name_x', 'competency', 'createdAt', 'updatedAt']].copy()
fact_skill_user.rename(columns={
    'createdAt' : 'user_createdAt',
    'updatedAt' : 'user_updatedAt',
    'department_name_x' : 'department_name'
}, inplace=True)


# FactCourseUser: Merge course_users with dim_user, and dim_course to get full information
fact_course_user = course_users_df.merge(dim_user, on='user_id', how='inner')\
                                  .merge(dim_course, on='course_id', how='inner')\
                                  .merge(course_departments_df, on='course_id', how='inner')\
                                  .merge(dim_department, on='department_id', how='inner')

fact_course_user = fact_course_user[['course_id', 'course_name', 'course_descripition', 'user_id_y', 'createdAt_y', 'updatedAt_y', 'user_id_x', 'first_name', 'last_name', 'account_type', 'department_id','department_name_x', 'user_score', 'createdAt_x', 'updatedAt_x', ]]
fact_course_user.rename(columns={
    'user_id_y': 'course_creator_id',
    'createdAt_y': 'course_createdAt',
    'updatedAt_y': 'course_updatedAt',
    'user_id_x': 'user_id',    
    'department_name_x': 'department_name',
    'createdAt_x': 'user_createdAt',
    'updatedAt_x': 'user_updatedAt'
}, inplace=True)


dim_user.to_csv('mart_DimUser.csv', index=False)
dim_department.to_csv('mart_DimDepartment.csv', index=False)
dim_skill.to_csv('mart_DimSkill.csv', index=False)
dim_course.to_csv('mart_DimCourse.csv', index=False)
fact_skill_user.to_csv('mart_FactSkillUser.csv', index=False)
fact_course_user.to_csv('mart_FactCourseUser.csv', index=False)

print("Data mart creation complete.")
