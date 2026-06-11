package service

import (
	"context"

	"github.com/brnathanlima/grpc-example/internal/database"
	"github.com/brnathanlima/grpc-example/internal/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
)

type CategoryService struct {
	pb.UnimplementedCategoryServiceServer
	CategoryDB database.Category
}

func NewCategoryService(db database.Category) *CategoryService {
	return &CategoryService{
		CategoryDB: db,
	}
}

func (c *CategoryService) CreateCategory(ctx context.Context, req *pb.CreateCategoryRequest) (*pb.Category, error) {
	category, err := c.CategoryDB.Create(req.Name, req.Description)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create category: %v", err)
	}

	categoryResponse := &pb.Category{
		Id:          category.ID,
		Name:        category.Name,
		Description: category.Description,
	}

	return categoryResponse, nil
}

func (c *CategoryService) ListCategories(ctx context.Context, req *pb.Blank) (*pb.CategoryList, error) {
	categories, err := c.CategoryDB.GetAll()
	if err != nil {
		return nil, err
	}

	var categoriesResponse []*pb.Category
	
	for _, category := range categories {
		categoryResponse := &pb.Category {
			Id: category.ID,
			Name: category.Name,
			Description: category.Description,
		}

		categoriesResponse = append(categoriesResponse, categoryResponse)
	}

	return &pb.CategoryList{Categories: categoriesResponse}, nil
}

func (c *CategoryService) GetCategory(ctx context.Context, req *pb.CategoryGetRequest) (*pb.Category, error) {
	category, err := c.CategoryDB.FindByID(req.Id)
	if err != nil {
		return nil, err
	}

	categoryResponse := &pb.Category {
		Id: category.ID,
		Name: category.Name,
		Description: category.Description,
	}

	return categoryResponse, nil
}

func (c *CategoryService) CreateCategoryStream(stream pb.CategoryService_CreateCategoryStreamServer) error {
	categories := &pb.CategoryList{}
	
	for {
		category, err := stream.Recv()

		if err == io.EOF {
			return stream.SendAndClose(categories)
		}
		if err != nil {
			return err
		}

		categoryResult, err := c.CategoryDB.Create(category.Name, category.Description)
		if err != nil {
			return err
		}
		
		categories.Categories = append(categories.Categories, &pb.Category{
			Id: categoryResult.ID,
			Name: categoryResult.Name,
			Description: categoryResult.Description,
		})
	}
}

func (c *CategoryService) CreateCategoryStreamBidirectional(stream pb.CategoryService_CreateCategoryStreamBidirectionalServer) error {
	for {
		category, err := stream.Recv()

		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		
		categoryResult, err := c.CategoryDB.Create(category.Name, category.Description)

		if err != nil {
			return err
		}

		if err := stream.Send(&pb.Category{
			Id: categoryResult.ID,
			Name: categoryResult.Name,
			Description: categoryResult.Description,
		}); err != nil {
			return err
		}
	}
}